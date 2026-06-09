package com.yg.scheduler.worker;

import com.yg.scheduler.common.CacheMigrationMessage;
import com.yg.scheduler.common.protocol.Message;
import com.yg.scheduler.common.protocol.ProtocolConstants;
import com.yg.scheduler.common.util.JsonUtil;
import com.yg.scheduler.common.JobContext;
import com.yg.scheduler.common.ExecutionResult;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler extends SimpleChannelInboundHandler<Message> {

    private final Set<String> executedTasks = ConcurrentHashMap.newKeySet();
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Connection established");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        switch (msg.getType()) {
            case ProtocolConstants.TYPE_HEARTBEAT:
                System.out.println("Heartbeat response received");
                break;
            case ProtocolConstants.TYPE_REQUEST:
                String json = new String(msg.getBody());
                System.out.println("Task received: " + json);

                JobContext job = JsonUtil.fromJson(json, JobContext.class);

                // 幂等检查
                if (executedTasks.contains(job.getTaskId())) {
                    System.out.println("Task already executed: " + job.getTaskId());
                    ExecutionResult duplicateResult = ExecutionResult.success(job.getJobId(), job.getTaskId(), "Already executed");
                    ctx.writeAndFlush(Message.response(JsonUtil.toJson(duplicateResult).getBytes()));
                    return;
                }

                System.out.println("Job ID: " + job.getJobId());
                System.out.println("Task ID: " + job.getTaskId());
                System.out.println("Job params: " + job.getParams());

                ExecutionResult result = execute(job);

                // 执行成功，记录已执行
                if (result.getSuccess()) {
                    executedTasks.add(job.getTaskId());
                }

                String resultJson = JsonUtil.toJson(result);
                ctx.writeAndFlush(Message.response(resultJson.getBytes()));
                System.out.println("Result sent: " + resultJson);
                break;

            // 缓存迁移消息
            case 101:
                String migrateJson = new String(msg.getBody());
                CacheMigrationMessage migrateMsg = JsonUtil.fromJson(migrateJson, CacheMigrationMessage.class);

                System.out.println("[Migration] Received cache migration request, keys count: " + migrateMsg.getHotKeys().size());

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

    private ExecutionResult execute(JobContext job) {
        // 分片感知预热：提前加载数据到本地缓存
        if (job.getPreloadKeys() != null && !job.getPreloadKeys().isEmpty()) {
            for (String key : job.getPreloadKeys()) {
                // 使用 cacheService.get 会自动加载到L1
                cacheService.get(key, () -> {
                    System.out.println("[Preload] Loading from DB: " + key);
                    return "{\"data\":\"preloaded\"}";
                });
            }
            System.out.println("[Preload] Preloaded " + job.getPreloadKeys().size() + " keys");
        }

        System.out.println("[DEBUG] execute() called, params=" + job.getParams());
        try {
            // 假设任务需要读取用户数据
            String userId = job.getParams();

//            // 故意加一个前缀，让布隆过滤器判断为不存在
//            String userData = cacheService.get("NOT_EXIST_" + userId, () -> {
//                System.out.println("[DB] Querying database for user: " + userId);
//                return null;  // 返回null，模拟数据不存在
//            });
//
//            System.out.println("User data from cache: " + userData);
            // 使用多级缓存读取数据
            String userData = cacheService.get("user:" + userId, () -> {
                // 模拟从数据库读取
                System.out.println("[DB] Querying database for user: " + userId);
                return "{\"name\":\"User" + userId + "\",\"level\":1}";
            });

            System.out.println("Executing job: " + job.getJobName() + ", shard: " + job.getShardingItem());
            System.out.println("User data from cache: " + userData);

            Thread.sleep(500);
            return ExecutionResult.success(job.getJobId(), job.getTaskId(), "Job executed successfully");
        } catch (Exception e) {
            return ExecutionResult.failure(job.getJobId(), job.getTaskId(), "Execution failed: " + e.getMessage());
        }
    }

    // 在 channelInactive 或关闭时清理资源
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cacheService.close();
        super.channelInactive(ctx);
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}