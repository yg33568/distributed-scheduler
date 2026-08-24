package com.yg.scheduler.worker;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.locks.Lock;

/**
 * 请求一个 Key →
 *   ① 布隆过滤器（最快）→ 已知不存在直接返回（防穿透）
 *   ② Caffeine本地缓存 L1（次快，本机内存）→ 找到就返回
 *   ③ 条带锁（防击穿）→ 同一 key 只让一个线程去查下一层
 *   ④ Redis L2（网络请求）→ 找到就返回
 *   ⑤ 数据库（最慢）→ 最后的选择
 *
 * 三个"防"：
 *   - 防穿透：布隆过滤器 + 空值(NULL)短缓存
 *   - 防击穿：固定条带锁（Striped），消除 per-key Map 的 remove 竞态
 *   - 防雪崩：写 Redis 时随机过期时间
 */
public class CacheService {

    private static final Logger log = LoggerFactory.getLogger(CacheService.class);

    /** 空值标记：布隆过滤器误判 / 数据确实不存在时，用短 TTL 的空值挡住重复打库 */
    private static final String NULL_MARKER = "NULL";

    /** 空值在 Redis 的存活秒数（防穿透，短一点） */
    private static final int NULL_TTL_SECONDS = 60;

    /** 数据基础过期秒数（防雪崩：加上随机偏移） */
    private static final int DATA_BASE_EXPIRE_SECONDS = 300;

    //Caffeine本地缓存（L1）
    private final Cache<String, String> localCache;

    //Redis连接池（L2）
    private final JedisPool jedisPool;

    // 1. 布隆过滤器（防穿透）：存放"已知存在"的 key 集合
    // 注意：必须先预热（prewarm），否则会误杀真实数据
    private final BloomFilter<String> bloomFilter;

    // 2. 固定条带锁（防击穿）：按 key 哈希取固定条带，同一 key 落到同一条带，
    //    无 unbounded map、无 remove 竞态；不同 key 可能共享条带（可接受的代价）
    private final Striped<Lock> keyLocks = Striped.lazyWeakLock(1024);

    // 3. 随机数生成器（防雪崩）
    private final Random random = new Random();

    // 布隆过滤器是否已预热。未预热时放行所有查询（避免拦截真实数据）
    private volatile boolean bloomWarm = false;

    public CacheService() {
        // 初始化本地缓存
        this.localCache = CacheConfig.createLocalCache();

        // 初始化Redis连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        this.jedisPool = new JedisPool(poolConfig, "localhost", 6379);

        // 初始化布隆过滤器（预计10万条数据，误判率1%）
        this.bloomFilter = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                100000,
                0.01
        );
    }

    /**
     * 预热布隆过滤器：把数据集中"已知存在"的 key 批量灌入。
     * 布隆过滤器是"已知存在集合"的硬门禁：只有预热完成后才启用过滤，
     * 否则新进来的真实 key 会被误判为不存在而直接返回 null。
     * 生产环境应从数据集/键注册表批量灌入；这里由 worker 在分片预热/缓存迁移时调用。
     */
    public void prewarm(Iterable<String> keys) {
        int count = 0;
        for (String key : keys) {
            bloomFilter.put(key);
            count++;
        }
        if (count > 0) {
            bloomWarm = true;
            log.info("[Cache] Bloom filter prewarmed with {} keys", count);
        }
    }

    public String get(String key, DataLoader dataLoader) {
        // ========== 第一层：布隆过滤器（防穿透） ==========
        // 预热完成后才启用门禁；"已知不存在"直接返回，避免打后面的层
        if (bloomWarm && !bloomFilter.mightContain(key)) {
            log.debug("[Cache] Bloom filter: key not exists, skip: {}", key);
            return null;
        }

        // ========== 第二层：本地缓存 L1 ==========
        String value = localCache.getIfPresent(key);
        if (value != null) {
            if (NULL_MARKER.equals(value)) {
                log.debug("[Cache] L1 hit but NULL marker: {}", key);
                return null;
            }
            log.debug("[Cache] L1 hit: {}", key);
            return value;
        }

        // ========== 第三层：条带锁（防击穿） ==========
        // 很多请求同时查同一个 Key 时，只有第一个请求去查 Redis/DB，其他请求等第一个查完再拿结果。
        Lock lock = keyLocks.get(key);
        lock.lock();
        try {
            // Double check：获取锁后再次检查本地缓存
            value = localCache.getIfPresent(key);
            if (value != null) {
                if (NULL_MARKER.equals(value)) {
                    return null;
                }
                return value;
            }

            // ========== 第四层：Redis分布式缓存 L2 ==========
            // 查到后回填本地缓存，下次就直接从本地拿。Redis 异常时降级，不阻断读。
            String redisValue = getFromRedis(key);
            if (redisValue != null) {
                localCache.put(key, redisValue);
                if (NULL_MARKER.equals(redisValue)) {
                    log.debug("[Cache] L2 hit but NULL marker: {}", key);
                    return null;
                }
                log.debug("[Cache] L2 hit: {}", key);
                return redisValue;
            }

            // ========== 第五层：数据库（DataLoader） ==========
            // 查到数据后写入 Redis 和本地缓存，下次就不用再查数据库了
            if (dataLoader != null) {
                value = dataLoader.load();
                if (value != null) {
                    // 防雪崩：随机过期时间
                    int expireTime = DATA_BASE_EXPIRE_SECONDS + random.nextInt(60);
                    writeToRedis(key, value, expireTime);
                    localCache.put(key, value);
                    bloomFilter.put(key);  // 数据确实存在，加入布隆过滤器
                    log.debug("[Cache] Loaded from DB and cached, expire: {}s", expireTime);
                } else {
                    // 空值缓存（防穿透），过期时间短一些
                    writeToRedis(key, NULL_MARKER, NULL_TTL_SECONDS);
                    localCache.put(key, NULL_MARKER);
                    log.debug("[Cache] NULL cached for key: {}", key);
                }
            }
            return value;
        } finally {
            lock.unlock();
        }
    }

    // 缓存失效方法
    public void evict(String key) {
        localCache.invalidate(key); // 清除本地缓存
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key); // 删除Redis中的Key
        } catch (Exception e) {
            log.warn("[Cache] Redis evict failed: {}", key, e);
        }
        log.info("[Cache] Evicted: {}", key);
    }

    //获取本地缓存命中率
    public double getHitRate() {
        return localCache.stats().hitRate();
    }

    //添加Key到布隆过滤器
    public void addToBloomFilter(String key) {
        bloomFilter.put(key);
    }

    //Redis 读取，异常降级返回 null（不阻断读）
    private String getFromRedis(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            log.warn("[Cache] Redis read failed, degrade to DB: {} err={}", key, e.toString());
            return null;
        }
    }

    //Redis 写入，异常仅告警（本地缓存仍生效）
    private void writeToRedis(String key, String value, int ttlSeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, ttlSeconds, value);
        } catch (Exception e) {
            log.warn("[Cache] Redis write failed (local cache still works): {} err={}", key, e.toString());
        }
    }

    public void close() {
        jedisPool.close();
    }

    @FunctionalInterface
    public interface DataLoader {
        String load();
    }
}
