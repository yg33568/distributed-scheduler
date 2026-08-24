package com.yg.scheduler.core;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * 一致性哈希路由器测试。
 * 重点验证 removeWorker 缓存迁移依赖的性质：节点下线后，其 key 必须被正确"后继节点"接管。
 */
public class ConsistentHashRouterTest {

    @Test
    public void testRouteReturnsValidNode() {
        ConsistentHashRouter router = new ConsistentHashRouter(java.util.List.of("A", "B", "C"), 150);
        for (int i = 0; i < 1000; i++) {
            String node = router.route("key-" + i);
            assertNotNull("key-" + i + " 必须路由到某个节点", node);
            assertTrue(node.equals("A") || node.equals("B") || node.equals("C"));
        }
    }

    @Test
    public void testRoutingIsDeterministic() {
        ConsistentHashRouter router = new ConsistentHashRouter(java.util.List.of("A", "B", "C"), 150);
        for (int i = 0; i < 100; i++) {
            String key = "shard-" + i;
            assertEquals("同一 key 多次路由结果必须一致", router.route(key), router.route(key));
        }
    }

    @Test
    public void testKeysSpreadAcrossNodes() {
        // 150 虚拟节点应让 key 分布均匀，不存在某个节点一个 key 都没有
        ConsistentHashRouter router = new ConsistentHashRouter(java.util.List.of("A", "B", "C"), 150);
        Set<String> used = new HashSet<>();
        for (int i = 0; i < 300; i++) {
            used.add(router.route("user:" + i));
        }
        assertEquals("key 应分布到所有节点", 3, used.size());
    }

    /**
     * 节点下线后，原属于它的 key 必须被重新路由到其他存活的节点（即哈希环上的后继者）。
     * 这正是 removeWorker 缓存迁移选择目标的依据。
     */
    @Test
    public void testRemoveNodeReassignsToSurvivors() {
        ConsistentHashRouter router = new ConsistentHashRouter(java.util.List.of("A", "B", "C"), 150);

        // 收集 300 个 key 中原本路由到 B 的那些
        Map<String, String> before = new HashMap<>();
        Set<String> keysOnB = new HashSet<>();
        for (int i = 0; i < 300; i++) {
            String key = "user:" + i;
            String node = router.route(key);
            before.put(key, node);
            if ("B".equals(node)) {
                keysOnB.add(key);
            }
        }
        assertFalse("本轮至少应有一些 key 路由到 B", keysOnB.isEmpty());

        // 移除 B 后，原 B 的 key 应全部落到 A 或 C，且不再指向 B
        router.removeNode("B");
        for (String key : keysOnB) {
            String after = router.route(key);
            assertNotNull(key, after);
            assertFalse("key " + key + " 不应再路由到已移除的 B", "B".equals(after));
            assertTrue("key " + key + " 应被 A 或 C 接管", after.equals("A") || after.equals("C"));
        }
    }

    @Test
    public void testAddNodeTakesSomeLoad() {
        ConsistentHashRouter router = new ConsistentHashRouter(java.util.List.of("A", "B"), 150);
        Set<String> keysOnA = new HashSet<>();
        for (int i = 0; i < 300; i++) {
            if ("A".equals(router.route("k" + i))) {
                keysOnA.add("k" + i);
            }
        }
        router.addNode("C");
        // 新增节点后，原 A 的 key 至少有一部分迁移到新节点 C
        boolean migratedToC = keysOnA.stream().anyMatch(k -> "C".equals(router.route(k)));
        assertTrue("新增节点后应有部分 key 迁移到 C", migratedToC);
    }
}
