package com.yg.scheduler.core;

import com.yg.scheduler.common.CacheMigrationMessage;
import com.yg.scheduler.common.config.AppConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

public class SchedulerServer {

    private static final Logger log = LoggerFactory.getLogger(SchedulerServer.class);

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

    //执行器注册
    public static void registerWorker(String workerId, ChannelHandlerContext ctx) {
        workers.put(workerId, ctx);
        rebuildRouter();
        log.info("Worker registered: {}, total workers: {}", workerId, workers.size());
    }

    /**
     * 执行器下线
     * 当执行器断开或心跳超时，做三件事：
     * ① 任务转移	把未完成的任务放入 retryQueue	任务不能丢
     * ② 缓存迁移	把热点Key发给其他执行器	新节点的缓存不能是冷的
     * ③ 移除节点	从 workers 移除，重建Hash环	后续任务不再发给它
     */
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
                log.info("Re-queued task {} from failed worker {}", job.getJobId(), workerId);
            }
        }

        // 缓存迁移
        // 获取该worker的热点key
        List<String> hotKeys = CacheMigrationService.getHotKeys(workerId);

        if (hotKeys != null && !hotKeys.isEmpty() && !workers.isEmpty()) {
            String targetWorkerId = workers.keySet().iterator().next();

            ChannelHandlerContext targetCtx = workers.get(targetWorkerId);
            if (targetCtx != null) {
                CacheMigrationMessage msg = new CacheMigrationMessage(hotKeys);
                String json = JsonUtil.toJson(msg);
                targetCtx.writeAndFlush(Message.cacheMigrate(json.getBytes()));
                log.info("[Migration] Migrated {} keys from {} to {}", hotKeys.size(), workerId, targetWorkerId);
            }
        }
        CacheMigrationService.removeHotKeys(workerId);

        // 移除节点
        workers.remove(workerId);
        rebuildRouter();
        log.info("Worker removed: {}, remaining workers: {}", workerId, workers.size());
    }

    private static void rebuildRouter() {
        if (workers.isEmpty()) {
            router = null;
            return;
        }
        List<String> nodeIds = new ArrayList<>(workers.keySet());
        router = new ConsistentHashRouter(nodeIds, 150);
        log.info("Router rebuilt with {} nodes", nodeIds.size());
    }

    public static Map<String, ChannelHandlerContext> getWorkersMap() {
        return workers;
    }


    //分片调度器
    //每 30 秒执行一次，生成 10 个分片任务，通过一致性Hash发给对应的执行器

    /**
     * 对每个分片：
     *     1. 用一致性哈希算出"该发给谁" → router.route("shard-0")
     *     2. 如果执行器在线 → 构建任务（JobContext）
     *     3. 记录热点Key（用于缓存迁移）
     *     4. 存数据库
     *     5. 发送给执行器
     *     6. 记录内存状态，设置30秒超时定时器
     */
    private void startShardingScheduler() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            if (workers.isEmpty() || router == null) {
                log.info("[Sharding] No workers, skip");
                return;
            }

            List<String> nodeIds = new ArrayList<>(workers.keySet());
            router = new ConsistentHashRouter(nodeIds, 150);

            int shardingTotal = 10;
            log.info("[Sharding] Start, total shards: {}", shardingTotal);

            for (int i = 0; i < shardingTotal; i++) {
                String shardKey = "shard-" + i;
                String targetWorkerId = router.route(shardKey);

                // 直接从 workers 中获取，打印 null 时的详细信息
                ChannelHandlerContext ctx = workers.get(targetWorkerId);
                if (ctx == null) {
                    log.warn("[Sharding] workers.get returned null for key: [{}]", targetWorkerId);
                    continue;
                }
                if (ctx != null && ctx.channel().isActive()) {
                    long jobId = System.currentTimeMillis() + i;
                    String taskId = jobId + "_" + i + "_0";
                    List<String> preloadKeys = new ArrayList<>();
                    for (int j = 0; j < 10; j++) {
                        preloadKeys.add("user:" + (i * 100 + j));
                    }

                    JobContext job = JobContext.builder()
                            .jobId(jobId)
                            .taskId(taskId)
                            .jobName("ShardTask")
                            .params("{\"shardIndex\":" + i + ",\"total\":" + shardingTotal + "}")
                            .shardingTotal(shardingTotal)
                            .shardingItem(i)
                            .timeout(30)
                            .retryCount(0)
                            .preloadKeys(preloadKeys)
                            .build();

                    // 生成热点key列表（用于缓存迁移）
                    List<String> hotKeys = new ArrayList<>();
                    for (int j = 0; j < 10; j++) {
                        hotKeys.add("user:" + (i * 100 + j));
                    }
                    CacheMigrationService.registerHotKeys(targetWorkerId, hotKeys);

                    String json = JsonUtil.toJson(job);
                    JobDao.getInstance().save(job, targetWorkerId);
                    ctx.writeAndFlush(Message.request(json.getBytes()));
                    log.info("[Sharding] Shard {} -> {}", i, targetWorkerId);

                    // 超时代码
                    pendingJobs.put(job.getJobId(), job);
                    runningTasks.computeIfAbsent(targetWorkerId, k -> new ArrayList<>()).add(job);

                    ScheduledFuture<?> timeoutFuture = timeoutExecutor.schedule(() -> {
                        JobContext timedOutJob = pendingJobs.remove(job.getJobId());
                        if (timedOutJob != null) {
                            log.warn("Job timeout: {}", job.getJobId());
                            JobDao.getInstance().updateStatus(job.getJobId(), "TIMEOUT", "Execution timeout");
                            if (timedOutJob.getRetryCount() == null || timedOutJob.getRetryCount() < 3) {
                                timedOutJob.setRetryCount(timedOutJob.getRetryCount() == null ? 1 : timedOutJob.getRetryCount() + 1);
                                retryQueue.offer(timedOutJob);
                            }
                        }
                    }, job.getTimeout(), TimeUnit.SECONDS);
                    timeoutFutures.put(job.getJobId(), timeoutFuture);
                }
            }
            log.info("[Sharding] End");
        }, 5, 30, TimeUnit.SECONDS);
    }

    //每 10 秒处理一个失败/超时的任务，重新发给其他执行器
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
                log.info("[Retry] Re-sending job {} to {}, retryCount={}", job.getJobId(), targetWorkerId, job.getRetryCount());
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

            log.info("SchedulerServer started on port {}", port);
            new Thread(() -> {
                try {
                    new AdminServer(AppConfig.getInt("scheduler.admin-port", 8081)).start();
                } catch (Exception e) {
                    log.error("AdminServer failed", e);
                }
            }).start();

            // 启动分片调度器
            startShardingScheduler();
            //启动重试调度器
            startRetryScheduler();

            ChannelFuture future = bootstrap.bind(port).sync();
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    //执行器返回结果后调用，负责清理资源。
    public static void onJobCompleted(Long jobId, boolean success, String message) {
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
                log.info("Added job {} to retry queue, retryCount={}", jobId, job.getRetryCount());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new SchedulerServer(AppConfig.getInt("scheduler.port", 8080)).start();
    }
}
