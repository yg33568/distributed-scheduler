package com.yg.scheduler.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashRouter {

    private final SortedMap<Integer, String> hashRing = new TreeMap<>();
    private final int virtualNodeCount;
    private final List<String> nodes;

    public ConsistentHashRouter(List<String> nodes, int virtualNodeCount) {
        this.virtualNodeCount = virtualNodeCount;
        this.nodes = new ArrayList<>(nodes);
        buildHashRing();
    }

    private void buildHashRing() {
        for (String node : nodes) {
            for (int i = 0; i < virtualNodeCount; i++) {
                String virtualNodeKey = node + "#" + i;
                int hash = hash(virtualNodeKey);
                hashRing.put(hash, node);
            }
        }
    }

    public String route(String key) {
        if (hashRing.isEmpty()) return null;
        int hash = hash(key);
        if (!hashRing.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = hashRing.tailMap(hash);
            hash = tailMap.isEmpty() ? hashRing.firstKey() : tailMap.firstKey();
        }
        return hashRing.get(hash);
    }

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