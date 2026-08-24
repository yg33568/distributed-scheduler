package com.yg.scheduler.core;

import com.yg.scheduler.common.zk.ZkClients;
import com.yg.scheduler.common.zk.ZkPaths;
import com.yg.scheduler.common.util.JsonUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Admin 控制台：leader/standby 都运行，用于查询集群状态、做健康检查。
 *   /status         集群整体状态（JSON）
 *   /leader         当前 leader 地址
 *   /workers        leader 视角的 worker 列表
 *   /jobs/pending   DB 中未完成任务数
 */
public class AdminHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LoggerFactory.getLogger(AdminHandler.class);

    // 共享的 ZK 客户端（只读 leader 节点），懒加载
    private static volatile CuratorFramework zk;

    private static CuratorFramework zk() {
        if (zk == null) {
            synchronized (AdminHandler.class) {
                if (zk == null) {
                    zk = ZkClients.newClient();
                    zk.start();
                }
            }
        }
        return zk;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        String uri = req.uri();
        String response;
        String contentType = "text/plain";

        try {
            switch (uri) {
                case "/status": {
                    SchedulerServer s = SchedulerServer.getInstance();
                    Map<String, Object> status = new LinkedHashMap<>();
                    status.put("role", (s != null && s.isLeader()) ? "LEADER" : "STANDBY");
                    status.put("isLeader", s != null && s.isLeader());
                    status.put("port", s != null ? s.getPort() : -1);
                    status.put("leader", readLeader());
                    status.put("workers", s != null ? s.getWorkersMap().keySet() : java.util.List.of());
                    status.put("pendingJobs", s != null ? s.getPendingJobsCount() : 0);
                    response = JsonUtil.toJson(status);
                    contentType = "application/json";
                    break;
                }
                case "/leader":
                    response = "leader=" + readLeader();
                    break;
                case "/workers": {
                    SchedulerServer s = SchedulerServer.getInstance();
                    if (s != null && s.isLeader()) {
                        response = "Workers: " + s.getWorkersMap().keySet();
                    } else {
                        response = "standby, leader=" + readLeader() + " (workers visible on leader)";
                    }
                    break;
                }
                case "/jobs/pending":
                    response = "Pending jobs count: " + JobDao.getInstance().findPendingJobs().size();
                    break;
                default:
                    response = "Not found. Use /status, /leader, /workers, /jobs/pending";
            }
        } catch (Exception e) {
            log.warn("Admin request failed: {}", uri, e);
            response = "error: " + e.toString();
        }

        FullHttpResponse res = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer(response, StandardCharsets.UTF_8)
        );
        res.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE);
    }

    private String readLeader() {
        try {
            byte[] data = zk().getData().forPath(ZkPaths.LEADER);
            return data == null || data.length == 0 ? "none" : new String(data, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "unknown (" + e.getClass().getSimpleName() + ")";
        }
    }
}
