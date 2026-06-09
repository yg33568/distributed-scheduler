package com.yg.scheduler.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheMigrationService {

    // 记录每个执行器负责的热点key
    private static final Map<String, List<String>> workerHotKeys = new ConcurrentHashMap<>();

    // 注册执行器的热点key
    public static void registerHotKeys(String workerId, List<String> hotKeys) {
        workerHotKeys.put(workerId, hotKeys);
        System.out.println("[Migration] Registered " + hotKeys.size() + " hot keys for worker " + workerId);
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