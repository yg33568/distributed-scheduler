package com.yg.scheduler.worker;

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
            default:
                System.out.println("Unknown message type: " + msg.getType());
        }
    }

//    private ExecutionResult execute(JobContext job) {
//        try {
//            System.out.println("Executing job: " + job.getJobName() + ", shard: " + job.getShardingItem());
//            Thread.sleep(500);
//            return ExecutionResult.success(job.getJobId(), job.getTaskId(), "Job executed successfully");
//        } catch (Exception e) {
//            return ExecutionResult.failure(job.getJobId(), job.getTaskId(), "Execution failed: " + e.getMessage());
//        }
//    }
private ExecutionResult execute(JobContext job) {
    try {
        System.out.println("Executing job: " + job.getJobName() + ", shard: " + job.getShardingItem());

        // 模拟超时：第5个分片故意执行60秒（超过30秒超时阈值）
        if (job.getShardingItem() == 5) {
            System.out.println("⚠️ Simulating timeout for shard 5, will take 60 seconds...");
            Thread.sleep(60000);  // 60秒，超过timeout=30秒
        } else {
            Thread.sleep(500);    // 正常任务0.5秒
        }

        return ExecutionResult.success(job.getJobId(), job.getTaskId(), "Job executed successfully");
    } catch (Exception e) {
        return ExecutionResult.failure(job.getJobId(), job.getTaskId(), "Execution failed: " + e.getMessage());
    }
}

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}