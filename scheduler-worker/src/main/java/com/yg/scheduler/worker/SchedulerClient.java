package com.yg.scheduler.worker;

import com.yg.scheduler.common.config.AppConfig;
import com.yg.scheduler.common.protocol.Message;
import com.yg.scheduler.common.protocol.MessageDecoder;
import com.yg.scheduler.common.protocol.MessageEncoder;
import com.yg.scheduler.common.util.JsonUtil;
import com.yg.scheduler.common.WorkerInfo;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Worker 客户端：通过 ZK 发现当前 leader，建立 TCP 长连接接收任务。
 *
 * 重连循环（master 集群下的 HA）：
 *   发现 leader → 连接 → 注册(等 ACK) → 长连接收任务 → 断线 → 指数退避+抖动 → 重新发现 leader
 *  - leader 切换时，LeaderDiscovery 拿到新地址，worker 自动连到新 leader 并重新注册
 *  - 幂等状态（executedTasks/executingTasks）与缓存都在 WorkerRuntime，跨重连不丢
 *  - workerId 持久化（配置 > 文件 > 生成），重启不漂移，保证一致性哈希亲和稳定
 */
public class SchedulerClient {

    private static final Logger log = LoggerFactory.getLogger(SchedulerClient.class);

    private final WorkerRuntime runtime;
    private volatile Channel channel;

    public SchedulerClient(WorkerRuntime runtime) {
        this.runtime = runtime;
    }

    public void run() throws Exception {
        LeaderDiscovery discovery = new LeaderDiscovery();
        discovery.start();

        NioEventLoopGroup group = new NioEventLoopGroup();
        startHeartbeat(); // 单条守护线程，跨重连复用

        int backoffMs = 1000;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                String leader = discovery.awaitLeader();
                if (leader == null) {
                    Thread.sleep(1000);
                    continue;
                }
                String[] hp = leader.split(":");
                if (hp.length < 2) {
                    log.warn("Invalid leader address: {}", leader);
                    Thread.sleep(1000);
                    continue;
                }
                try {
                    connectOnce(group, hp[0], Integer.parseInt(hp[1]));
                    // 连接成功过（哪怕是短暂），恢复正常节奏，避免退避被无限放大
                    backoffMs = 1000;
                    Thread.sleep(1000 + ThreadLocalRandom.current().nextInt(200));
                } catch (Exception e) {
                    // 连接失败（leader 可能挂了/还没就绪）→ 指数退避 + 抖动，防重连风暴
                    log.warn("Connect to leader {} failed: {}", leader, e.toString());
                    Thread.sleep(backoffMs + ThreadLocalRandom.current().nextInt(300));
                    backoffMs = Math.min(backoffMs * 2, 5000);
                }
            }
        } finally {
            discovery.close();
            group.shutdownGracefully();
        }
    }

    private void connectOnce(NioEventLoopGroup group, String host, int port) throws Exception {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new MessageDecoder());
                        pipeline.addLast(new MessageEncoder());
                        pipeline.addLast(new ClientHandler(runtime));
                    }
                });

        ChannelFuture future = bootstrap.connect(host, port).sync();
        this.channel = future.channel();
        log.info("Connected to leader {}:{}", host, port);

        // 发送注册消息，等 master 回 ACK（确认已加入哈希环）
        runtime.resetRegisterAck();
        String advertiseHost = AppConfig.get("worker.advertise-host", host);
        WorkerInfo info = new WorkerInfo(runtime.getWorkerId(), advertiseHost, port, true, System.currentTimeMillis());
        String json = JsonUtil.toJson(info);
        channel.writeAndFlush(Message.register(json.getBytes()));
        log.info("Registered with workerId: {}", runtime.getWorkerId());
        if (!runtime.awaitRegisterAck(10, TimeUnit.SECONDS)) {
            log.warn("Register ACK not received within 10s, continuing anyway");
        }

        // 阻塞直到连接断开（leader 挂 / 网络中断），断开后回到 run 循环重连
        future.channel().closeFuture().sync();
    }

    // 单条心跳线程，跨重连复用
    private void startHeartbeat() {
        Thread heartbeatThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(30000);
                    Channel ch = channel;
                    if (ch != null && ch.isActive()) {
                        ch.writeAndFlush(Message.heartbeat());
                        log.debug("Send heartbeat");
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    log.debug("Heartbeat send failed: {}", e.toString());
                }
            }
        }, "worker-heartbeat");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    /**
     * workerId 解析：配置/环境变量 WORKER_ID 优先 → 持久化文件(重启不漂移) → 时间戳生成兜底。
     * workerId 稳定对一致性哈希很重要：id 变化会导致该 worker 负责的分片范围变化。
     */
    private static String resolveWorkerId() {
        String id = AppConfig.get("worker.id", "");
        if (id != null && !id.isEmpty()) {
            return id;
        }
        Path file = Paths.get(".worker-id");
        try {
            if (Files.exists(file)) {
                String saved = Files.readString(file, StandardCharsets.UTF_8).trim();
                if (!saved.isEmpty()) {
                    return saved;
                }
            }
            String generated = "worker-" + System.currentTimeMillis();
            Files.write(file, generated.getBytes(StandardCharsets.UTF_8));
            return generated;
        } catch (IOException e) {
            log.warn("Failed to persist worker id, using ephemeral", e);
            return "worker-" + System.currentTimeMillis();
        }
    }

    public static void main(String[] args) throws Exception {
        String workerId = resolveWorkerId();
        WorkerRuntime runtime = new WorkerRuntime(workerId);
        Runtime.getRuntime().addShutdownHook(new Thread(runtime::close, "worker-shutdown"));
        new SchedulerClient(runtime).run();
    }
}
