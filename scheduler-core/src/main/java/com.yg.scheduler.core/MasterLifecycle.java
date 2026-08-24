package com.yg.scheduler.core;

import com.yg.scheduler.common.config.AppConfig;
import com.yg.scheduler.common.zk.ZkClients;
import com.yg.scheduler.common.zk.ZkPaths;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Master 生命周期管理：ZooKeeper 选主 + 主备切换。
 *
 * 主备模式（Active-Standby）：
 *  - 每个 master 进程持有本对象，通过 LeaderLatch 竞争 /scheduler/leader-latch
 *  - 成为 leader → 启动 TCP、调度器、任务恢复，并向 /scheduler/leader 写入自己的地址
 *  - 失去 leader（leader 挂了 ZK 触发选举 / 被降级）→ 关闭 TCP 与调度器、删除 leader 节点，
 *    防止旧 leader 继续调度造成脑裂
 *  - 备机保持待命，仅运行 Admin HTTP（可查询状态）
 */
public class MasterLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MasterLifecycle.class);

    private final SchedulerServer server;
    private final CuratorFramework client;
    private final LeaderLatch leaderLatch;
    private final String advertiseHost;
    private final int schedulerPort;

    /** 当前实例是否在 /scheduler/leader 上写了自己的地址（只有写过才允许删除） */
    private volatile boolean hasLeaderNode = false;

    public MasterLifecycle(SchedulerServer server) {
        this.server = server;
        this.advertiseHost = AppConfig.get("scheduler.advertise-host", "localhost");
        this.schedulerPort = server.getPort();
        this.client = ZkClients.newClient();
        // LeaderLatch 的 id 仅用于日志标识
        this.leaderLatch = new LeaderLatch(client, ZkPaths.LEADER_LATCH, advertiseHost + ":" + schedulerPort);
    }

    public void start() throws Exception {
        client.start();
        if (!client.blockUntilConnected(15, TimeUnit.SECONDS)) {
            throw new IllegalStateException("ZooKeeper not reachable at " + AppConfig.get("zk.connect", "localhost:2181"));
        }

        leaderLatch.start();
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.info(">>>> BECAME LEADER ({}:{})", advertiseHost, schedulerPort);
                try {
                    becomeLeader();
                } catch (Exception e) {
                    log.error("Become leader failed", e);
                    server.shutdown();
                }
            }

            @Override
            public void notLeader() {
                log.info("<<<< NOT LEADER, standby mode ({}:{})", advertiseHost, schedulerPort);
                // 失去 leader：关闭调度，删除 leader 节点，避免旧 leader 继续调度（脑裂）
                if (hasLeaderNode) {
                    try {
                        client.delete().quietly().forPath(ZkPaths.LEADER);
                    } catch (Exception e) {
                        log.warn("Failed to delete leader node", e);
                    }
                    hasLeaderNode = false;
                }
                server.shutdown();
            }
        });
        log.info("MasterLifecycle started, waiting for leadership...");
    }

    private void becomeLeader() throws Exception {
        server.setLeader(true);
        // ① 接收 worker 连接
        server.startTcpServer();
        // ② 启动分片/重试调度
        server.startSchedulers();
        // ③ 恢复未完成任务（worker 端幂等兜底避免重复执行）
        server.startRecovery();
        // ④ 把本节点地址写进 /scheduler/leader（worker 据此连接）
        String leaderAddr = advertiseHost + ":" + schedulerPort;
        if (client.checkExists().forPath(ZkPaths.LEADER) == null) {
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(ZkPaths.LEADER, leaderAddr.getBytes(StandardCharsets.UTF_8));
        } else {
            client.setData().forPath(ZkPaths.LEADER, leaderAddr.getBytes(StandardCharsets.UTF_8));
        }
        hasLeaderNode = true;
        log.info("Advertised leader address at {} = {}", ZkPaths.LEADER, leaderAddr);
    }

    public void close() {
        try {
            if (hasLeaderNode) {
                client.delete().quietly().forPath(ZkPaths.LEADER);
            }
        } catch (Exception ignored) {
        }
        server.shutdown();
        try {
            leaderLatch.close();
        } catch (Exception ignored) {
        }
        client.close();
        log.info("MasterLifecycle closed");
    }
}
