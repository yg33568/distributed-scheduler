package com.yg.scheduler.worker;

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

//调度客户端/Worker入口
public class SchedulerClient {

    private final String host;
    private final int port;
    private Channel channel;

    public SchedulerClient(String host, int port) {
        this.host = host;
        this.port = port;
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
                            pipeline.addLast(new ClientHandler());
                        }
                    });

            ChannelFuture future = bootstrap.connect(host, port).sync();
            this.channel = future.channel();
            System.out.println("Connected to scheduler: " + host + ":" + port);

            // 发送注册消息
            String workerId = "worker-" + System.currentTimeMillis();//基于时间戳生成唯一ID
            WorkerInfo info = new WorkerInfo(workerId, host, port, true, System.currentTimeMillis());
            String json = JsonUtil.toJson(info);
            channel.writeAndFlush(Message.register(json.getBytes()));
            System.out.println("Registered with workerId: " + workerId);

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
                    System.out.println("Send heartbeat");
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatThread.setDaemon(true);// 设置为守护线程，JVM退出时自动终止
        heartbeatThread.start();
    }

    public static void main(String[] args) throws Exception {
        new SchedulerClient("127.0.0.1", 8080).connect();
    }
}