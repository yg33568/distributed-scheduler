package com.yg.scheduler.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkerInfo {
    private String workerId;      // 执行器ID，如 "worker-1"
    private String host;          // IP地址
    private int port;             // 端口
    private boolean healthy;      // 是否健康
    private long lastHeartbeat;   // 最后心跳时间
}