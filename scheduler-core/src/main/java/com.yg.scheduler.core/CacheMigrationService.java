package com.yg.scheduler.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//缓存迁移服务
public class CacheMigrationService {

    private static final Logger log = LoggerFactory.getLogger(CacheMigrationService.class);

    // 记录每个执行器负责的热点key
    private static final Map<String, List<String>> workerHotKeys = new ConcurrentHashMap<>();

    // 注册执行器的热点key
    public static void registerHotKeys(String workerId, List<String> hotKeys) {
        workerHotKeys.put(workerId, hotKeys);
        log.info("[Migration] Registered {} hot keys for worker {}", hotKeys.size(), workerId);
    }

    // 获取执行器的热点key（用于故障迁移）
    public static List<String> getHotKeys(String workerId) {
        return workerHotKeys.get(workerId);
    }

    // 移除执行器的热点key
    public static void removeHotKeys(String workerId) {
        workerHotKeys.remove(workerId);
    }
}