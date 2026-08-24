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

//调度客户端/Worker入口
public class SchedulerClient {

    private static final Logger log = LoggerFactory.getLogger(SchedulerClient.class);

    private final String host;
    private final int port;
    private final WorkerRuntime runtime;
    private Channel channel;

    public SchedulerClient(String host, int port, WorkerRuntime runtime) {
        this.host = host;
        this.port = port;
        this.runtime = runtime;
    }

    //连接调度服务器
    public void connect() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup();

        try {
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
            log.info("Connected to scheduler: {}:{}", host, port);

            // 发送注册消息
            String advertiseHost = AppConfig.get("worker.advertise-host", host);
            WorkerInfo info = new WorkerInfo(runtime.getWorkerId(), advertiseHost, port, true, System.currentTimeMillis());
            String json = JsonUtil.toJson(info);
            channel.writeAndFlush(Message.register(json.getBytes()));
            log.info("Registered with workerId: {}", runtime.getWorkerId());

            startHeartbeat();

            future.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    //启动心跳线程
    private void startHeartbeat() {
        Thread heartbeatThread = new Thread(() -> {
            while (channel != null && channel.isActive()) {
                try {
                    Thread.sleep(30000);
                    channel.writeAndFlush(Message.heartbeat());
                    log.debug("Send heartbeat");
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);// 设置为守护线程，JVM退出时自动终止
        heartbeatThread.start();
    }

    public static void main(String[] args) throws Exception {
        String host = AppConfig.get("scheduler.host", "localhost");
        int port = AppConfig.getInt("scheduler.port", 8080);
        String workerId = AppConfig.get("worker.id", "");
        if (workerId == null || workerId.isEmpty()) {
            workerId = "worker-" + System.currentTimeMillis();
        }

        WorkerRuntime runtime = new WorkerRuntime(workerId);
        // 进程退出时释放 Redis 连接池等资源（不再在每次断线时关闭缓存）
        Runtime.getRuntime().addShutdownHook(new Thread(runtime::close, "worker-shutdown"));

        new SchedulerClient(host, port, runtime).connect();
    }
}
