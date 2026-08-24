package com.yg.scheduler.worker;

import com.yg.scheduler.common.zk.ZkClients;
import com.yg.scheduler.common.zk.ZkPaths;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Leader 服务发现：监听 ZK 下 /scheduler/leader 临时节点的变化。
 *
 *  - leader 节点数据 = "host:port"（master 成为 leader 时写入）
 *  - leader 更换（原 leader 挂 / 被降级）时，worker 拿到新地址，重连即可
 *  - 这就是 worker 在 master 集群下的"服务发现"：不再写死一个 master 地址
 */
public class LeaderDiscovery {

    private static final Logger log = LoggerFactory.getLogger(LeaderDiscovery.class);

    private final CuratorFramework client;
    private volatile String leaderAddress;

    public LeaderDiscovery() {
        this.client = ZkClients.newClient();
    }

    public void start() throws Exception {
        client.start();
        if (!client.blockUntilConnected(15, TimeUnit.SECONDS)) {
            throw new IllegalStateException("ZooKeeper not reachable at " + com.yg.scheduler.common.config.AppConfig.get("zk.connect", "localhost:2181"));
        }

        PathChildrenCache cache = new PathChildrenCache(client, ZkPaths.ROOT, true);
        cache.getListenable().addListener((curator, event) -> {
            ChildData data = event.getData();
            if (data == null) {
                return;
            }
            switch (event.getType()) {
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    if (ZkPaths.LEADER.equals(data.getPath())) {
                        leaderAddress = new String(data.getData(), StandardCharsets.UTF_8);
                        log.info("Leader discovered: {}", leaderAddress);
                    }
                    break;
                case CHILD_REMOVED:
                    if (ZkPaths.LEADER.equals(data.getPath())) {
                        log.info("Leader node removed, waiting for new leader...");
                        // 置空，让重连循环等待新 leader 写入
                        leaderAddress = null;
                    }
                    break;
                default:
                    break;
            }
        });
        cache.start();
        log.info("LeaderDiscovery watching {}", ZkPaths.LEADER);
    }

    /** 当前 leader 地址（host:port），无 leader 时为 null */
    public String currentLeader() {
        return leaderAddress;
    }

    /** 阻塞等待 leader 出现（500ms 轮询），用于重连循环 */
    public String awaitLeader() throws InterruptedException {
        String addr;
        while ((addr = leaderAddress) == null) {
            Thread.sleep(500);
        }
        return addr;
    }

    public void close() {
        client.close();
    }
}
