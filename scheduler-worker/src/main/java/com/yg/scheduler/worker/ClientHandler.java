package com.yg.scheduler.worker;

import com.yg.scheduler.common.CacheMigrationMessage;
import com.yg.scheduler.common.protocol.Message;
import com.yg.scheduler.common.protocol.ProtocolConstants;
import com.yg.scheduler.common.util.JsonUtil;
import com.yg.scheduler.common.JobContext;
import com.yg.scheduler.common.ExecutionResult;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

//负责接收调度中心发来的所有消息，并执行对应的操作。
public class ClientHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    // worker 进程共享运行时（幂等状态、缓存跨重连不丢）
    private final WorkerRuntime runtime;

    public ClientHandler(WorkerRuntime runtime) {
        this.runtime = runtime;
    }

    // 连接建立时
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Connection established");
    }

    //收到消息时的处理
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        switch (msg.getType()) {
            //心跳响应
            case ProtocolConstants.TYPE_HEARTBEAT:
                log.debug("Heartbeat response received");
                break;

            //注册确认：master 确认 worker 已加入哈希环
            case ProtocolConstants.TYPE_REGISTER_ACK:
                log.info("Register ACK received, worker is now on the hash ring");
                break;

            //任务请求
            case ProtocolConstants.TYPE_REQUEST:
                String json = new String(msg.getBody());
                log.debug("Task received: {}", json);

                // 1. 解析 JSON → JobContext 对象
                JobContext job = JsonUtil.fromJson(json, JobContext.class);

                // 2. 幂等检查一：已完成 → 直接回成功
                if (runtime.getExecutedTasks().contains(job.getTaskId())) {
                    log.info("Task already executed: {}", job.getTaskId());
                    ctx.writeAndFlush(Message.response(
                            JsonUtil.toJson(ExecutionResult.success(job.getJobId(), job.getTaskId(), "Already executed")).getBytes()));
                    return;
                }
                // 3. 幂等检查二：正在执行（leader 故障重派到本节点）→ 回成功，避免重复执行/重派死循环
                if (!runtime.getExecutingTasks().add(job.getTaskId())) {
                    log.info("Task already executing: {}", job.getTaskId());
                    ctx.writeAndFlush(Message.response(
                            JsonUtil.toJson(ExecutionResult.success(job.getJobId(), job.getTaskId(), "Already executing")).getBytes()));
                    return;
                }

                // 4. 执行任务
                ExecutionResult result;
                try {
                    result = execute(job);
                } finally {
                    runtime.getExecutingTasks().remove(job.getTaskId());
                }

                // 5. 执行成功，记录已执行（幂等）
                if (result.getSuccess()) {
                    runtime.getExecutedTasks().add(job.getTaskId());
                }

                // 6. 返回结果给调度中心
                ctx.writeAndFlush(Message.response(JsonUtil.toJson(result).getBytes()));
                log.debug("Result sent: {}", JsonUtil.toJson(result));
                break;

            // 缓存迁移消息
            case ProtocolConstants.TYPE_CACHE_MIGRATE:
                String migrateJson = new String(msg.getBody());
                // 收到迁移消息，取出 hotKeys 列表
                CacheMigrationMessage migrateMsg = JsonUtil.fromJson(migrateJson, CacheMigrationMessage.class);
                log.info("[Migration] Received cache migration request, keys count: {}", migrateMsg.getHotKeys().size());

                // 这些 key 在原 worker 上是热点，确实存在 → 先预热布隆过滤器，避免被门禁拦掉
                runtime.getCacheService().prewarm(migrateMsg.getHotKeys());

                // 逐个预加载到本地缓存
                for (String key : migrateMsg.getHotKeys()) {
                    runtime.getCacheService().get(key, () -> {
                        log.info("[Migration] Loading migrated key from DB: {}", key);
                        return "{\"migrated\":true}";
                    });
                }
                log.info("[Migration] Preloaded {} keys from failed worker", migrateMsg.getHotKeys().size());
                break;

            default:
                log.warn("Unknown message type: {}", msg.getType());
        }
    }

    //execute — 真正执行任务的地方
    // 数据模型：分片 i 负责处理 user:{i*100} .. user:{i*100+9} 这 10 个 key，
    // 与调度中心生成 preloadKeys / hotKeys 的规则一致，保证缓存预热真正生效。
    private ExecutionResult execute(JobContext job) {
        int base = job.getShardingItem() * 100;
        List<String> keys = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
            keys.add("user:" + (base + j));
        }

        log.debug("execute() called, shard={}, keys={}", job.getShardingItem(), keys);

        try {
            // ① 预热布隆过滤器：这些 key 是数据集中确实存在的（与调度中心的 preloadKeys 一致）
            runtime.getCacheService().prewarm(keys);

            // ② 分片感知预热：逐个读取，加载到 L1/L2 缓存
            for (String key : keys) {
                runtime.getCacheService().get(key, () -> {
                    log.debug("[DB] Querying database for: {}", key);
                    String uid = key.substring("user:".length());
                    return "{\"name\":\"User" + uid + "\",\"level\":1}";
                });
            }

            // ③ 业务读取（命中预热的 L1 缓存）
            String userData = runtime.getCacheService().get("user:" + (base + 3), () -> "{\"name\":\"unknown\"}");
            log.info("Executing job: {}, shard: {}", job.getJobName(), job.getShardingItem());
            log.info("User data from cache: {}", userData);

            Thread.sleep(500);
            return ExecutionResult.success(job.getJobId(), job.getTaskId(), "Job executed successfully");
        } catch (Exception e) {
            return ExecutionResult.failure(job.getJobId(), job.getTaskId(), "Execution failed: " + e.getMessage());
        }
    }

    // 发生异常时
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception: {}", cause.toString());
        ctx.close();
    }
}
