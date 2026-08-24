package com.yg.scheduler.core;

import com.yg.scheduler.common.CacheMigrationMessage;
import com.yg.scheduler.common.config.AppConfig;
import com.yg.scheduler.common.protocol.MessageDecoder;
import com.yg.scheduler.common.protocol.MessageEncoder;
import com.yg.scheduler.common.util.JsonUtil;
import com.yg.scheduler.common.JobContext;
import com.yg.scheduler.common.protocol.Message;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 调度中心（Scheduler）。
 *
 * 集群模式（主备 Active-Standby）下：
 *  - 多个 SchedulerServer 进程通过 ZooKeeper 选主（MasterLifecycle），只有 leader 才：
 *      startTcpServer() 接收 worker 连接、startSchedulers() 跑分片/重试调度、startRecovery() 恢复任务
 *  - leader 挂掉后，ZK 选举触发，备机接管：重建哈希环（worker 重连注册）、从 DB 恢复任务
 *  - 所有内存状态改为实例字段 + 单例，便于被降级时干净 shutdown（避免旧 leader 继续调度造成脑裂）
 */
public class SchedulerServer {

    private static final Logger log = LoggerFactory.getLogger(SchedulerServer.class);

    private static volatile SchedulerServer instance;

    private final int port;

    // ===== 全部运行时状态改为实例字段（集群下每实例一份，leader 切换即重建） =====
    private final Map<String, ChannelHandlerContext> workers = new ConcurrentHashMap<>();
    private volatile ConsistentHashRouter router;
    private final Map<Long, JobContext> pendingJobs = new ConcurrentHashMap<>();
    private final Map<Long, ScheduledFuture<?>> timeoutFutures = new ConcurrentHashMap<>();
    private final Map<String, List<JobContext>> runningTasks = new ConcurrentHashMap<>();
    private final Queue<JobContext> retryQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();

    // 实例随机盐：jobId = (now<<16) | (salt+i)，避免同毫秒生成冲突（脑裂双实例/多次调度）
    private final int instanceSalt = ThreadLocalRandom.current().nextInt(0xFFFF);

    // ===== 生命周期：保存引用以便降级/退出时干净关闭 =====
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile ScheduledExecutorService shardingExecutor;
    private volatile ScheduledExecutorService retryExecutor;
    private volatile boolean leader = false;

    private SchedulerServer(int port) {
        this.port = port;
    }

    public static void init(int port) {
        if (instance == null) {
            synchronized (SchedulerServer.class) {
                if (instance == null) {
                    instance = new SchedulerServer(port);
                }
            }
        }
    }

    public static SchedulerServer getInstance() {
        return instance;
    }

    public int getPort() {
        return port;
    }

    public boolean isLeader() {
        return leader;
    }

    public void setLeader(boolean leader) {
        this.leader = leader;
    }

    // ===== 静态门面：供 ServerHandler / AdminHandler 调用，内部委托给单例 =====

    public static void registerWorker(String workerId, ChannelHandlerContext ctx) {
        SchedulerServer s = instance;
        if (s != null) {
            s.doRegisterWorker(workerId, ctx);
        }
    }

    public static void removeWorker(String workerId, ChannelHandlerContext ctx) {
        SchedulerServer s = instance;
        if (s != null) {
            s.doRemoveWorker(workerId, ctx);
        }
    }

    public static void onJobCompleted(Long jobId, boolean success, String message) {
        SchedulerServer s = instance;
        if (s != null) {
            s.doOnJobCompleted(jobId, success, message);
        }
    }

    public static Map<String, ChannelHandlerContext> getWorkersMap() {
        SchedulerServer s = instance;
        return s == null ? Map.of() : s.workers;
    }

    /** 内存中待响应/执行中的任务数（admin 监控用） */
    public int getPendingJobsCount() {
        return pendingJobs.size();
    }

    // ===== 生命周期：仅 leader 调用 =====

    /**
     * 启动 TCP 服务（接收 worker 注册/心跳/任务结果）。
     * 在后台线程绑定，bind 完成后把 serverChannel 保存下来，供 shutdown 关闭。
     */
    public synchronized void startTcpServer() throws Exception {
        if (serverChannel != null && serverChannel.isActive()) {
            log.info("TCP server already running on port {}", port);
            return;
        }
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

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

        // bind 是异步的，sync() 等待绑定完成
        serverChannel = bootstrap.bind(port).sync().channel();
        log.info("SchedulerServer TCP listening on port {}", port);
    }

    /** 启动分片调度 + 重试调度，保存 executor 引用便于 shutdown */
    public synchronized void startSchedulers() {
        if (shardingExecutor != null && !shardingExecutor.isShutdown()) {
            log.info("Schedulers already running");
            return;
        }
        shardingExecutor = Executors.newSingleThreadScheduledExecutor();
        retryExecutor = Executors.newSingleThreadScheduledExecutor();
        startShardingScheduler(shardingExecutor);
        startRetryScheduler(retryExecutor);
        log.info("Sharding & retry schedulers started");
    }

    /** 停止调度器（被降级或关闭时调用，防止旧 leader 继续调度造成脑裂） */
    public synchronized void stopSchedulers() {
        shutdownQuietly(shardingExecutor);
        shutdownQuietly(retryExecutor);
        shardingExecutor = null;
        retryExecutor = null;
    }

    /** 恢复未完成任务：leader 接管后从 DB reload PENDING/RUNNING 任务重新派发 */
    public void startRecovery() {
        new JobRecovery(this).recover();
    }

    /** 关闭 TCP + 调度器 + 清空内存状态（被降级 / 进程退出） */
    public synchronized void shutdown() {
        log.info("Shutting down scheduler (leader={})", leader);
        leader = false;
        stopSchedulers();
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
        workers.clear();
        pendingJobs.clear();
        timeoutFutures.clear();
        runningTasks.clear();
        retryQueue.clear();
        router = null;
        log.info("Scheduler shutdown complete");
    }

    private static void shutdownQuietly(ExecutorService executor) {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdownNow();
        }
    }

    // ===== 注册/下线/结果处理 =====

    private void doRegisterWorker(String workerId, ChannelHandlerContext ctx) {
        workers.put(workerId, ctx);
        rebuildRouter();
        log.info("Worker registered: {}, total workers: {}", workerId, workers.size());
    }

    /**
     * 执行器下线：任务转移 + 缓存迁移 + 移除节点重建哈希环
     */
    private void doRemoveWorker(String workerId, ChannelHandlerContext ctx) {
        // 防误删：若该 workerId 当前注册的 channel 不是发起移除的这个连接，说明是陈旧连接，忽略
        if (workers.get(workerId) != ctx) {
            log.info("Stale removal ignored for worker {}, current channel differs", workerId);
            return;
        }

        // ① 任务转移：将该 worker 未完成的任务放回重试队列
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

        // ② 缓存迁移：先把节点从哈希环移除并重建环，再对每个热点key用一致性哈希求"后继者"，
        //    按后继者聚合后发迁移指令，保证迁移目标是哈希环上的正确下一个节点
        workers.remove(workerId);
        rebuildRouter();
        List<String> hotKeys = CacheMigrationService.getHotKeys(workerId);
        if (hotKeys != null && !hotKeys.isEmpty() && router != null) {
            Map<String, List<String>> migrateTargets = new HashMap<>();
            for (String key : hotKeys) {
                String targetWorkerId = router.route(key);
                if (targetWorkerId != null) {
                    migrateTargets.computeIfAbsent(targetWorkerId, k -> new ArrayList<>()).add(key);
                }
            }
            for (Map.Entry<String, List<String>> e : migrateTargets.entrySet()) {
                ChannelHandlerContext targetCtx = workers.get(e.getKey());
                if (targetCtx != null) {
                    CacheMigrationMessage msg = new CacheMigrationMessage(e.getValue());
                    String json = JsonUtil.toJson(msg);
                    targetCtx.writeAndFlush(Message.cacheMigrate(json.getBytes()));
                    log.info("[Migration] Migrated {} keys from {} to {}", e.getValue().size(), workerId, e.getKey());
                }
            }
        }
        CacheMigrationService.removeHotKeys(workerId);

        // ③ 移除节点
        log.info("Worker removed: {}, remaining workers: {}", workerId, workers.size());
    }

    private void rebuildRouter() {
        if (workers.isEmpty()) {
            router = null;
            return;
        }
        List<String> nodeIds = new ArrayList<>(workers.keySet());
        router = new ConsistentHashRouter(nodeIds, 150);
        log.info("Router rebuilt with {} nodes", nodeIds.size());
    }

    private void doOnJobCompleted(Long jobId, boolean success, String message) {
        ScheduledFuture<?> timeoutFuture = timeoutFutures.remove(jobId);
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        JobContext job = pendingJobs.remove(jobId);
        if (job != null) {
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

    /**
     * 统一任务派发入口：分片调度、重试调度、故障恢复三处共用，保证行为一致。
     * 1. 一致性哈希路由 targetWorkerId = router.route(taskId)
     * 2. 目标在线 → 存 DB、发送、登记 pendingJobs/runningTasks、设超时定时器
     * 3. 目标不在线 → 推回重试队列
     *
     * @return true 表示成功派发给某个 worker
     */
    public boolean dispatchJob(JobContext job) {
        if (workers.isEmpty() || router == null) {
            retryQueue.offer(job);
            return false;
        }
        String targetWorkerId = router.route(job.getTaskId());
        ChannelHandlerContext ctx = workers.get(targetWorkerId);
        if (ctx == null || !ctx.channel().isActive()) {
            retryQueue.offer(job);
            log.info("[Dispatch] No active target for job {}, queued for retry", job.getJobId());
            return false;
        }

        String json = JsonUtil.toJson(job);
        JobDao.getInstance().save(job, targetWorkerId);
        ctx.writeAndFlush(Message.request(json.getBytes()));
        log.info("[Dispatch] Job {} (shard={}) -> {}", job.getJobId(), job.getShardingItem(), targetWorkerId);

        pendingJobs.put(job.getJobId(), job);
        runningTasks.computeIfAbsent(targetWorkerId, k -> new ArrayList<>()).add(job);

        int timeout = job.getTimeout() == null ? 30 : job.getTimeout();
        ScheduledFuture<?> timeoutFuture = timeoutExecutor.schedule(() -> {
            JobContext timedOut = pendingJobs.remove(job.getJobId());
            if (timedOut != null) {
                log.warn("Job timeout: {}", job.getJobId());
                JobDao.getInstance().updateStatus(job.getJobId(), "TIMEOUT", "Execution timeout");
                if (timedOut.getRetryCount() == null || timedOut.getRetryCount() < 3) {
                    timedOut.setRetryCount(timedOut.getRetryCount() == null ? 1 : timedOut.getRetryCount() + 1);
                    retryQueue.offer(timedOut);
                }
            }
        }, timeout, TimeUnit.SECONDS);
        timeoutFutures.put(job.getJobId(), timeoutFuture);
        return true;
    }

    // ===== 调度器 =====

    // 每 30 秒生成 10 个分片任务，通过一致性Hash发给对应的执行器
    private void startShardingScheduler(ScheduledExecutorService scheduler) {
        scheduler.scheduleAtFixedRate(() -> {
            if (workers.isEmpty() || router == null) {
                log.info("[Sharding] No workers, skip");
                return;
            }

            int shardingTotal = 10;
            log.info("[Sharding] Start, total shards: {}", shardingTotal);

            for (int i = 0; i < shardingTotal; i++) {
                long jobId = (System.currentTimeMillis() << 16) | ((instanceSalt + i) & 0xFFFF);
                String taskId = jobId + "_" + i + "_0";

                List<String> preloadKeys = new ArrayList<>();
                List<String> hotKeys = new ArrayList<>();
                for (int j = 0; j < 10; j++) {
                    String key = "user:" + (i * 100 + j);
                    preloadKeys.add(key);
                    hotKeys.add(key);
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

                // 按 taskId 路由（与 dispatchJob 内部一致），把热点key登记到真正收任务的 worker 名下
                String targetWorkerId = router.route(job.getTaskId());
                if (targetWorkerId != null) {
                    CacheMigrationService.registerHotKeys(targetWorkerId, hotKeys);
                }

                dispatchJob(job);
            }
            log.info("[Sharding] End");
        }, 5, 30, TimeUnit.SECONDS);
    }

    // 每 10 秒处理一个失败/超时的任务，重新发给其他执行器
    private void startRetryScheduler(ScheduledExecutorService scheduler) {
        scheduler.scheduleAtFixedRate(() -> {
            if (retryQueue.isEmpty() || workers.isEmpty()) return;

            JobContext job = retryQueue.poll();
            if (job == null) return;

            if (dispatchJob(job)) {
                log.info("[Retry] Re-sent job {}, retryCount={}", job.getJobId(), job.getRetryCount());
            } else {
                log.info("[Retry] Re-dispatch failed, job {} requeued, retryCount={}", job.getJobId(), job.getRetryCount());
            }
        }, 5, 10, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception {
        int port = AppConfig.getInt("scheduler.port", 8080);
        SchedulerServer.init(port);
        SchedulerServer server = SchedulerServer.getInstance();

        // Admin HTTP 在 leader/standby 上都运行，便于查询任意节点状态
        new Thread(() -> {
            try {
                new AdminServer(AppConfig.getInt("scheduler.admin-port", 8081)).start();
            } catch (Exception e) {
                log.error("AdminServer failed", e);
            }
        }, "admin-server").start();

        // 参与 ZK 选主：leader 才启动 TCP + 调度 + 恢复
        MasterLifecycle lifecycle = new MasterLifecycle(server);
        Runtime.getRuntime().addShutdownHook(new Thread(lifecycle::close, "master-shutdown"));
        lifecycle.start();

        // 主线程常驻，进程生命周期由 shutdown hook 结束
        Thread.currentThread().join();
    }
}
