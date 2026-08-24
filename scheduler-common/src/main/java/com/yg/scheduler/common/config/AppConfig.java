package com.yg.scheduler.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 统一配置中心。
 *
 * 读取优先级：环境变量（Docker / K8s 场景） > classpath 下 application.properties > 代码默认值。
 * 约定：配置 key 用小写点/横线（如 db.jdbc-url），对应环境变量为全大写下划线（DB_JDBC_URL）。
 *
 * 这样同一套代码既能本地直接跑（读 properties），又能容器化部署（环境变量覆盖），
 * 不再把 MySQL/Redis/ZK 地址硬编码进业务代码。
 */
public final class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    private static final Properties props = new Properties();

    static {
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
                log.info("Loaded application.properties, {} entries", props.size());
            } else {
                log.info("No application.properties on classpath, fall back to env/defaults");
            }
        } catch (IOException e) {
            log.warn("Failed to load application.properties", e);
        }
    }

    private AppConfig() {
    }

    /** 读取字符串配置：环境变量优先，其次 properties 文件，最后默认值 */
    public static String get(String key, String def) {
        String env = System.getenv(envName(key));
        if (env != null && !env.isEmpty()) {
            return env;
        }
        return props.getProperty(key, def);
    }

    public static int getInt(String key, int def) {
        String raw = get(key, String.valueOf(def));
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid int for key {}, value={}, use default {}", key, raw, def);
            return def;
        }
    }

    public static long getLong(String key, long def) {
        String raw = get(key, String.valueOf(def));
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid long for key {}, value={}, use default {}", key, raw, def);
            return def;
        }
    }

    public static boolean getBoolean(String key, boolean def) {
        return Boolean.parseBoolean(get(key, String.valueOf(def)));
    }

    /** 配置 key -> 环境变量名：小写点/横线转全大写下划线，如 db.jdbc-url -> DB_JDBC_URL */
    private static String envName(String key) {
        return key.replace('.', '_').replace('-', '_').toUpperCase();
    }
}
