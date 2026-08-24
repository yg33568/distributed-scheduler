package com.yg.scheduler.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

//一致性哈希路由器，用来决定一个分片任务应该发给哪个执行器。
public class ConsistentHashRouter {

    // TreeMap保证按哈希值排序，便于查找
    private final SortedMap<Integer, String> hashRing = new TreeMap<>();
    private final int virtualNodeCount; //虚拟节点数量，默认150
    private final List<String> nodes; //真实节点列表

    public ConsistentHashRouter(List<String> nodes, int virtualNodeCount) {
        this.virtualNodeCount = virtualNodeCount;
        this.nodes = new ArrayList<>(nodes);
        buildHashRing();
    }

    //构建哈希环，把每个真实节点映射成150个虚拟节点，均匀分布在环上
    private void buildHashRing() {
        for (String node : nodes) {
            for (int i = 0; i < virtualNodeCount; i++) {
                String virtualNodeKey = node + "#" + i;
                int hash = hash(virtualNodeKey); // 计算虚拟节点的哈希值
                hashRing.put(hash, node); // 存入哈希环：哈希值 -> 真实节点ID
            }
        }
    }

    //根据 key（如 "shard-0"）找到负责它的执行器
    public String route(String key) {
        if (hashRing.isEmpty()) return null;
        int hash = hash(key);
        if (!hashRing.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = hashRing.tailMap(hash); //查找所有大于等于该哈希值的节点
            hash = tailMap.isEmpty() ? hashRing.firstKey() : tailMap.firstKey();// 取最小的那个（环形的下一个节点）
        }
        return hashRing.get(hash);
    }

    //MD5哈希计算方法
    private int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return ((digest[3] & 0xFF) << 24) |
                    ((digest[2] & 0xFF) << 16) |
                    ((digest[1] & 0xFF) << 8) |
                    (digest[0] & 0xFF);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    //动态增减执行器，只影响相邻节点，大部分数据不需要重新分配
    public void addNode(String node) {
        nodes.add(node);
        for (int i = 0; i < virtualNodeCount; i++) {
            String virtualNodeKey = node + "#" + i;
            int hash = hash(virtualNodeKey);
            hashRing.put(hash, node);
        }
    }

    public void removeNode(String node) {
        nodes.remove(node);
        for (int i = 0; i < virtualNodeCount; i++) {
            String virtualNodeKey = node + "#" + i;
            int hash = hash(virtualNodeKey);
            hashRing.remove(hash);
        }
    }
}