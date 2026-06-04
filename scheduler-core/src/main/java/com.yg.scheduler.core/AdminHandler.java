package com.yg.scheduler.core;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import java.nio.charset.StandardCharsets;

public class AdminHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        String uri = req.uri();
        String response;

        if (uri.equals("/workers")) {
            response = "Workers: " + SchedulerServer.getWorkersMap().keySet();
        } else if (uri.equals("/jobs/pending")) {
            response = "Pending jobs count: " + JobDao.getInstance().findPendingJobs().size();
        } else {
            response = "Not found. Use /workers or /jobs/pending";
        }

        FullHttpResponse res = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer(response, StandardCharsets.UTF_8)
        );
        res.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
    }
}