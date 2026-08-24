package com.yg.scheduler.common.config;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 配置中心测试。
 * 注意：环境变量无法在 JVM 内模拟，这里覆盖"properties 文件读取"与"默认值兜底"两条链路，
 * 环境变量优先级逻辑在 Docker 部署里通过运行时验证。
 */
public class AppConfigTest {

    @Test
    public void testReadsPropertiesFile() {
        assertEquals("hello-from-props", AppConfig.get("test.key", "fallback"));
    }

    @Test
    public void testDefaultFallback() {
        assertEquals("fallback", AppConfig.get("nonexistent.key", "fallback"));
        assertEquals(8080, AppConfig.getInt("nonexistent.port", 8080));
        assertTrue(AppConfig.getBoolean("nonexistent.flag", true));
        assertFalse(AppConfig.getBoolean("nonexistent.flag2", false));
    }

    @Test
    public void testInvalidIntFallsBackToDefault() {
        // test.invalid-int 在测试 properties 里是"not-a-number"，应回退默认值 42
        assertEquals(42, AppConfig.getInt("test.invalid-int", 42));
    }
}
