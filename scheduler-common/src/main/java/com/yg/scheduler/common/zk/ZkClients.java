package com.yg.scheduler.common.zk;

import com.yg.scheduler.common.config.AppConfig;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Curator 客户端工厂。
 * 连接串与会话超时来自配置（zk.connect / zk.session-timeout-ms），Docker 场景由环境变量覆盖。
 */
public final class ZkClients {

    private ZkClients() {
    }

    public static CuratorFramework newClient() {
        String connectString = AppConfig.get("zk.connect", "localhost:2181");
        int sessionTimeoutMs = AppConfig.getInt("zk.session-timeout-ms", 10000);
        // 指数退避重试：ZK 短暂不可用时自动重连，不让 JVM 直接崩
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        return CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
    }
}
