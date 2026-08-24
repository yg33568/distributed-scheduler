package com.yg.scheduler.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;

// CacheMessage 是一个缓存变更通知
// 用于在多个执行器节点之间同步缓存状态
// 解决多执行器之间的缓存一致性问题
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheMessage implements Serializable {
    private String cacheKey; //需要操作的缓存Key
    private String operation; //操作类型（"EVICT" 删除 / "UPDATE" 更新）
    private long timestamp; //消息时间戳
}