package com.yg.scheduler.worker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker 进程内共享的运行时状态。
 * 一个 worker 进程对应一个 WorkerRuntime，在断线重连 / leader 切换过程中保持不变：
 *   - workerId：节点标识（重启不漂移，保证哈希亲和稳定）
 *   - executedTasks：已完成任务集合（幂等：防止重复执行）
 *   - executingTasks：执行中任务集合（幂等：leader 故障重派到本节点时防重派死循环）
 *   - cacheService：多级缓存（重连不复建，Redis 连接池/布隆过滤器共享）
 */
public class WorkerRuntime {

    private final String workerId;
    private final Set<String> executedTasks = ConcurrentHashMap.newKeySet();
    private final Set<String> executingTasks = ConcurrentHashMap.newKeySet();
    private final CacheService cacheService;

    public WorkerRuntime(String workerId) {
        this.workerId = workerId;
        this.cacheService = new CacheService();
    }

    public String getWorkerId() {
        return workerId;
    }

    public Set<String> getExecutedTasks() {
        return executedTasks;
    }

    public Set<String> getExecutingTasks() {
        return executingTasks;
    }

    public CacheService getCacheService() {
        return cacheService;
    }

    public void close() {
        cacheService.close();
    }
}
