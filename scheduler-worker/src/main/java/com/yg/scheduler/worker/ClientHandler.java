package com.yg.scheduler.worker;

import com.yg.scheduler.common.CacheMigrationMessage;
import com.yg.scheduler.common.protocol.Message;
import com.yg.scheduler.common.protocol.ProtocolConstants;
import com.yg.scheduler.common.util.JsonUtil;
import com.yg.scheduler.common.JobContext;
import com.yg.scheduler.common.ExecutionResult;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

//负责接收调度中心发来的所有消息，并执行对应的操作。
public class ClientHandler extends SimpleChannelInboundHandler<Message> {

    //已执行任务集合（用于幂等性判断）
    private final Set<String> executedTasks = ConcurrentHashMap.newKeySet();

    // 连接建立时
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Connection established");
    }

    //收到消息时的处理
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        switch (msg.getType()) {
            //心跳响应
            case ProtocolConstants.TYPE_HEARTBEAT:
                System.out.println("Heartbeat response received");
                break;
            //任务请求
            case ProtocolConstants.TYPE_REQUEST:
                String json = new String(msg.getBody());
                System.out.println("Task received: " + json);

                // 1. 解析 JSON → JobContext 对象
                JobContext job = JsonUtil.fromJson(json, JobContext.class);

                // 2.幂等检查：如果这个 taskId 已经执行过，直接返回成功
                if (executedTasks.contains(job.getTaskId())) {
                    System.out.println("Task already executed: " + job.getTaskId());
                    ExecutionResult duplicateResult = ExecutionResult.success(job.getJobId(), job.getTaskId(), "Already executed");
                    ctx.writeAndFlush(Message.response(JsonUtil.toJson(duplicateResult).getBytes()));
                    return;
                }

                System.out.println("Job ID: " + job.getJobId());
                System.out.println("Task ID: " + job.getTaskId());
                System.out.println("Job params: " + job.getParams());

                // 3. 执行任务
                ExecutionResult result = execute(job);

                // 4.执行成功，记录已执行
                if (result.getSuccess()) {
                    executedTasks.add(job.getTaskId());
                }

                String resultJson = JsonUtil.toJson(result);

                // 5. 返回结果给调度中心
                ctx.writeAndFlush(Message.response(resultJson.getBytes()));
                System.out.println("Result sent: " + resultJson);
                break;

            // 缓存迁移消息
            case 101:
                String migrateJson = new String(msg.getBody());
                // 收到迁移消息，取出 hotKeys 列表
                CacheMigrationMessage migrateMsg = JsonUtil.fromJson(migrateJson, CacheMigrationMessage.class);
                System.out.println("[Migration] Received cache migration request, keys count: " + migrateMsg.getHotKeys().size());

                // 这些 key 在原 worker 上是热点，确实存在 → 先预热布隆过滤器，避免被门禁拦掉
                cacheService.prewarm(migrateMsg.getHotKeys());

                // 逐个预加载到本地缓存
                for (String key : migrateMsg.getHotKeys()) {
                    cacheService.get(key, () -> {
                        System.out.println("[Migration] Loading migrated key from DB: " + key);
                        return "{\"migrated\":true}";
                    });
                }
                System.out.println("[Migration] Preloaded " + migrateMsg.getHotKeys().size() + " keys from failed worker");
                break;

            default:
                System.out.println("Unknown message type: " + msg.getType());
        }
    }

    private final CacheService cacheService = new CacheService();

    //execute — 真正执行任务的地方
    // 数据模型：分片 i 负责处理 user:{i*100} .. user:{i*100+9} 这 10 个 key，
    // 与调度中心生成 preloadKeys / hotKeys 的规则一致，保证缓存预热真正生效。
    private ExecutionResult execute(JobContext job) {
        int base = job.getShardingItem() * 100;
        List<String> keys = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
            keys.add("user:" + (base + j));
        }

        System.out.println("[DEBUG] execute() called, shard=" + job.getShardingItem() + ", keys=" + keys);

        try {
            // ① 预热布隆过滤器：这些 key 是数据集中确实存在的（与调度中心的 preloadKeys 一致）
            cacheService.prewarm(keys);

            // ② 分片感知预热：逐个读取，加载到 L1/L2 缓存
            for (String key : keys) {
                cacheService.get(key, () -> {
                    System.out.println("[DB] Querying database for: " + key);
                    String uid = key.substring("user:".length());
                    return "{\"name\":\"User" + uid + "\",\"level\":1}";
                });
            }

            // ③ 业务读取（命中预热的 L1 缓存）
            String userData = cacheService.get("user:" + (base + 3), () -> "{\"name\":\"unknown\"}");
            System.out.println("Executing job: " + job.getJobName() + ", shard: " + job.getShardingItem());
            System.out.println("User data from cache: " + userData);

            Thread.sleep(500);
            return ExecutionResult.success(job.getJobId(), job.getTaskId(), "Job executed successfully");
        } catch (Exception e) {
            return ExecutionResult.failure(job.getJobId(), job.getTaskId(), "Execution failed: " + e.getMessage());
        }
    }

    // 在连接断开或关闭时清理资源
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cacheService.close(); // 释放 Redis 连接池等资源
        super.channelInactive(ctx);
    }
    // 发生异常时
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}