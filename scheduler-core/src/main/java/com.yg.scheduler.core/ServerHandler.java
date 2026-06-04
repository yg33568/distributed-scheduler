package com.yg.scheduler.core;

import com.yg.scheduler.common.ExecutionResult;
import com.yg.scheduler.common.protocol.Message;
import com.yg.scheduler.common.protocol.ProtocolConstants;
import com.yg.scheduler.common.util.JsonUtil;
import com.yg.scheduler.common.WorkerInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

public class ServerHandler extends SimpleChannelInboundHandler<Message> {

    private String workerId;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Executor connected: " + ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Executor disconnected: " + ctx.channel().remoteAddress());
        if (workerId != null) {
            SchedulerServer.removeWorker(workerId);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        switch (msg.getType()) {
            case ProtocolConstants.TYPE_HEARTBEAT:
                System.out.println("Heartbeat from " + ctx.channel().remoteAddress());
                ctx.writeAndFlush(Message.heartbeat());
                break;

            case ProtocolConstants.TYPE_RESPONSE:
                String resultJson = new String(msg.getBody());
                ExecutionResult result = JsonUtil.fromJson(resultJson, ExecutionResult.class);

                if (result.getJobId() != null) {
                    if (result.getSuccess()) {
                        JobDao.getInstance().updateStatus(result.getJobId(), "SUCCESS", null);
                        System.out.println("Updated job " + result.getJobId() + " status to SUCCESS");
                    } else {
                        JobDao.getInstance().updateStatus(result.getJobId(), "FAILED", result.getMessage());
                        System.out.println("Updated job " + result.getJobId() + " status to FAILED");
                    }
                    // 通知调度中心完成任务
                    // 需要获取 SchedulerServer 实例来调用 onJobCompleted
                }
                break;

            case ProtocolConstants.TYPE_REGISTER:
                String json = new String(msg.getBody());
                WorkerInfo workerInfo = JsonUtil.fromJson(json, WorkerInfo.class);
                this.workerId = workerInfo.getWorkerId();
                SchedulerServer.registerWorker(workerId, ctx);
                break;

            default:
                System.out.println("Unknown message type: " + msg.getType());
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            System.out.println("Heartbeat timeout, closing: " + ctx.channel().remoteAddress());
            if (workerId != null) {
                SchedulerServer.removeWorker(workerId);
            }
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        if (workerId != null) {
            SchedulerServer.removeWorker(workerId);
        }
        ctx.close();
    }
}