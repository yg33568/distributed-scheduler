package com.yg.scheduler.core;

import com.yg.scheduler.common.protocol.MessageDecoder;
import com.yg.scheduler.common.protocol.MessageEncoder;
import com.yg.scheduler.common.util.JsonUtil;
import com.yg.scheduler.common.JobContext;
import com.yg.scheduler.common.protocol.Message;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

public class SchedulerServer {

    private final int port;
    private static final Map<String, ChannelHandlerContext> workers = new ConcurrentHashMap<>();
    private static ConsistentHashRouter router;

    // 待响应任务 + 超时控制
    private static final Map<Long, JobContext> pendingJobs = new ConcurrentHashMap<>();
    private static final Map<Long, ScheduledFuture<?>> timeoutFutures = new ConcurrentHashMap<>();
    // 运行中的任务（按 worker 分组）
    private static final Map<String, List<JobContext>> runningTasks = new ConcurrentHashMap<>();
    // 失败重试队列
    private static final Queue<JobContext> retryQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();

    public SchedulerServer(int port) {
        this.port = port;
    }

    public static void registerWorker(String workerId, ChannelHandlerContext ctx) {
        workers.put(workerId, ctx);
        rebuildRouter();
        System.out.println("Worker registered: " + workerId + ", total workers: " + workers.size());
    }

    public static void removeWorker(String workerId) {
        // 任务转移：将该 worker 未完成的任务放回重试队列
        List<JobContext> orphanTasks = runningTasks.remove(workerId);
        if (orphanTasks != null && !orphanTasks.isEmpty()) {
            for (JobContext job : orphanTasks) {
                pendingJobs.remove(job.getJobId());
                ScheduledFuture<?> timeoutFuture = timeoutFutures.remove(job.getJobId());
                if (timeoutFuture != null) {
                    timeoutFuture.cancel(false);
                }
                JobDao.getInstance().updateStatus(job.getJobId(), "PENDING", "Worker removed, pending retry");
                retryQueue.offer(job);
                System.out.println("Re-queued task " + job.getJobId() + " from failed worker " + workerId);
            }
        }

        workers.remove(workerId);
        rebuildRouter();
        System.out.println("Worker removed: " + workerId + ", remaining workers: " + workers.size());
    }

    private static void rebuildRouter() {
        if (workers.isEmpty()) {
            router = null;
            return;
        }
        List<String> nodeIds = new ArrayList<>(workers.keySet());
        router = new ConsistentHashRouter(nodeIds, 150);
        System.out.println("Router rebuilt with " + nodeIds.size() + " nodes");
    }

    public static Map<String, ChannelHandlerContext> getWorkersMap() {
        return workers;
    }

    private void startShardingScheduler() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            if (workers.isEmpty() || router == null) {
                System.out.println("[Sharding] No workers, skip");
                return;
            }

            List<String> nodeIds = new ArrayList<>(workers.keySet());
            router = new ConsistentHashRouter(nodeIds, 150);

            int shardingTotal = 10;
            System.out.println("[Sharding] Start, total shards: " + shardingTotal);

            for (int i = 0; i < shardingTotal; i++) {
                String shardKey = "shard-" + i;
                String targetWorkerId = router.route(shardKey);

                // 直接从 workers 中获取，打印 null 时的详细信息
                ChannelHandlerContext ctx = workers.get(targetWorkerId);
                if (ctx == null) {
                    System.out.println("[DEBUG] workers.get returned null for key: [" + targetWorkerId + "]");
                    // 尝试遍历 keys 找出真正匹配的
                    for (String key : workers.keySet()) {
                        System.out.println("[DEBUG] comparing with key: [" + key + "], equals=" + key.equals(targetWorkerId));
                    }
                    continue;
                }
                if (ctx != null && ctx.channel().isActive()) {
                    long jobId = System.currentTimeMillis() + i;
                    String taskId = jobId + "_" + i + "_0";  // 格式：jobId_shardingItem_retryCount

                    JobContext job = JobContext.builder()
                            .jobId(jobId)
                            .taskId(taskId)
                            .jobName("ShardTask")
                            .params("{\"shardIndex\":" + i + ",\"total\":" + shardingTotal + "}")
                            .shardingTotal(shardingTotal)
                            .shardingItem(i)
                            .timeout(30)
                            .retryCount(0)
                            .build();

                    String json = JsonUtil.toJson(job);
                    JobDao.getInstance().save(job, targetWorkerId);
                    ctx.writeAndFlush(Message.request(json.getBytes()));
                    System.out.println("[Sharding] Shard " + i + " -> " + targetWorkerId);

                    // ========== 超时代码放在这里 ==========
                    pendingJobs.put(job.getJobId(), job);
                    runningTasks.computeIfAbsent(targetWorkerId, k -> new ArrayList<>()).add(job);

                    ScheduledFuture<?> timeoutFuture = timeoutExecutor.schedule(() -> {
                        JobContext timedOutJob = pendingJobs.remove(job.getJobId());
                        if (timedOutJob != null) {
                            System.out.println("Job timeout: " + job.getJobId());
                            JobDao.getInstance().updateStatus(job.getJobId(), "TIMEOUT", "Execution timeout");
                            if (timedOutJob.getRetryCount() == null || timedOutJob.getRetryCount() < 3) {
                                timedOutJob.setRetryCount(timedOutJob.getRetryCount() == null ? 1 : timedOutJob.getRetryCount() + 1);
                                retryQueue.offer(timedOutJob);
                            }
                        }
                    }, job.getTimeout(), TimeUnit.SECONDS);
                    timeoutFutures.put(job.getJobId(), timeoutFuture);
                    // ===================================
                }
            }
            System.out.println("[Sharding] End");
        }, 5, 30, TimeUnit.SECONDS);
    }

    private void startRetryScheduler() {
        ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();
        retryScheduler.scheduleAtFixedRate(() -> {
            if (retryQueue.isEmpty() || workers.isEmpty()) return;

            List<String> nodeIds = new ArrayList<>(workers.keySet());
            router = new ConsistentHashRouter(nodeIds, 150);

            JobContext job = retryQueue.poll();
            if (job == null) return;

            String targetWorkerId = router.route(job.getTaskId());
            ChannelHandlerContext ctx = workers.get(targetWorkerId);

            if (ctx != null && ctx.channel().isActive()) {
                String json = JsonUtil.toJson(job);
                ctx.writeAndFlush(Message.request(json.getBytes()));
                System.out.println("[Retry] Re-sending job " + job.getJobId() + " to " + targetWorkerId + ", retryCount=" + job.getRetryCount());
            } else {
                retryQueue.offer(job);
            }
        }, 5, 10, TimeUnit.SECONDS);
    }

    public void start() throws Exception {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new IdleStateHandler(60, 0, 0));
                            pipeline.addLast(new MessageDecoder());
                            pipeline.addLast(new MessageEncoder());
                            pipeline.addLast(new ServerHandler());
                        }
                    });

            System.out.println("SchedulerServer started on port " + port);
            new Thread(() -> {
                try {
                    new AdminServer(8081).start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

            // 启动分片调度器
            startShardingScheduler();
            startRetryScheduler();

            ChannelFuture future = bootstrap.bind(port).sync();
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public void onJobCompleted(Long jobId, boolean success, String message) {
        ScheduledFuture<?> timeoutFuture = timeoutFutures.remove(jobId);
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        JobContext job = pendingJobs.remove(jobId);
        if (job != null) {
            // 从 runningTasks 中移除
            for (List<JobContext> tasks : runningTasks.values()) {
                tasks.removeIf(t -> t.getJobId().equals(jobId));
            }
            JobDao.getInstance().updateStatus(jobId, success ? "SUCCESS" : "FAILED", message);
            if (!success && (job.getRetryCount() == null || job.getRetryCount() < 3)) {
                job.setRetryCount(job.getRetryCount() == null ? 1 : job.getRetryCount() + 1);
                retryQueue.offer(job);
                System.out.println("Added job " + jobId + " to retry queue, retryCount=" + job.getRetryCount());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new SchedulerServer(8080).start();
    }
}