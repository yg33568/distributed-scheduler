package com.yg.scheduler.core;

import com.yg.scheduler.common.ExecutionResult;
import com.yg.scheduler.common.protocol.Message;
import com.yg.scheduler.common.protocol.ProtocolConstants;
import com.yg.scheduler.common.util.JsonUtil;
import com.yg.scheduler.common.WorkerInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ServerHandler 是调度中心（Scheduler）的业务处理器
// 负责处理所有来自执行器（Worker）的消息
public class ServerHandler extends SimpleChannelInboundHandler<Message> {

    private static final Logger log = LoggerFactory.getLogger(ServerHandler.class);

    private String workerId;

    //执行器连接时
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Executor connected: {}", ctx.channel().remoteAddress());
    }

    //执行器断开时
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Executor disconnected: {}", ctx.channel().remoteAddress());
        if (workerId != null) {
            SchedulerServer.removeWorker(workerId);// 从注册表移除
        }
    }

    //收到消息时的处理
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        switch (msg.getType()) {
            case ProtocolConstants.TYPE_HEARTBEAT:
                ctx.writeAndFlush(Message.heartbeat());
                break;

            /**
             * 执行器执行完任务后返回结果，调度中心调用 onJobCompleted 做三件事：
             * 取消超时定时器
             * 从 pendingJobs 和 runningTasks 中移除任务
             * 更新数据库状态（成功/失败）
             */
            case ProtocolConstants.TYPE_RESPONSE:
                String resultJson = new String(msg.getBody());
                ExecutionResult result = JsonUtil.fromJson(resultJson, ExecutionResult.class);

                if (result.getJobId() != null) {
                    SchedulerServer.onJobCompleted(
                            result.getJobId(),
                            result.getSuccess(),
                            result.getSuccess() ? null : result.getMessage()
                    );
                    log.info("Job {} completed, success={}", result.getJobId(), result.getSuccess());
                }
                break;

            //执行器启动时发送注册消息，调度中心把它加入 workers 列表，重建一致性Hash环。
            case ProtocolConstants.TYPE_REGISTER:
                String json = new String(msg.getBody());
                WorkerInfo workerInfo = JsonUtil.fromJson(json, WorkerInfo.class);
                this.workerId = workerInfo.getWorkerId();
                SchedulerServer.registerWorker(workerId, ctx);
                // 回注册确认，worker 拿到 ACK 才认为注册成功
                ctx.writeAndFlush(Message.registerAck());
                break;

            default:
                log.warn("Unknown message type: {}", msg.getType());
        }
    }

    //空闲超时（60秒无消息）
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            log.warn("Heartbeat timeout, closing: {}", ctx.channel().remoteAddress());
            if (workerId != null) {
                SchedulerServer.removeWorker(workerId);// 移除节点
            }

            ctx.close();
        }
    }

    //发生异常时
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception on channel {}: {}", ctx.channel().remoteAddress(), cause.toString());
        if (workerId != null) {
            SchedulerServer.removeWorker(workerId);
        }
        ctx.close();
    }
}
