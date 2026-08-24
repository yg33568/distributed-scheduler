package com.yg.scheduler.worker;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 请求一个 Key →
 *   ① 布隆过滤器（最快，12MB内存）→ 不存在直接返回
 *   ② Caffeine本地缓存（次快，本机内存）→ 找到就返回
 *   ③ 互斥锁（防击穿）→ 只让一个线程去查
 *   ④ Redis（网络请求）→ 找到就返回
 *   ⑤ 数据库（最慢）→ 最后的选择
 */
public class CacheService {

    //Caffeine本地缓存（L1）
    private final Cache<String, String> localCache;

    //Redis连接池
    private final JedisPool jedisPool;

    // 1. 布隆过滤器（防穿透）
    private final BloomFilter<String> bloomFilter;

    // 2. 互斥锁（防击穿）
    private final Map<String, ReentrantLock> keyLocks = new ConcurrentHashMap<>();

    // 3. 随机数生成器（防雪崩）
    private final Random random = new Random();

    public CacheService() {
        System.out.println("[DEBUG] CacheService constructor called");

        // 初始化本地缓存
        this.localCache = CacheConfig.createLocalCache();

        // 初始化Redis连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        this.jedisPool = new JedisPool(poolConfig, "localhost", 6380);

        // 初始化布隆过滤器（预计10万条数据，误判率1%）
        this.bloomFilter = BloomFilter.create(
                Funnels.stringFunnel(StandardCharsets.UTF_8),
                100000,
                0.01
        );
    }

    public String get(String key, DataLoader dataLoader) {
        // ========== 第一层：布隆过滤器（防穿透） ==========
        if (!bloomFilter.mightContain(key)) {
            System.out.println("[Cache] Bloom filter: key not exists, skip: " + key);
            return null;
        }

        // ========== 第二层：本地缓存 L1 ==========
        String value = localCache.getIfPresent(key);
        if (value != null) {
            // 检查是否是空值标记（布隆过滤器误判）
            if ("NULL".equals(value)) {
                System.out.println("[Cache] Bloom filter false positive, key not exists: " + key);
                return null;
            }
            System.out.println("[Cache] L1 hit: " + key);
            return value;
        }
        System.out.println("[Cache] L1 miss: " + key);

        // ========== 第三层：互斥锁（防击穿） ==========
        //如果很多请求同时查同一个 Key，只有第一个请求去查 Redis/DB，其他请求等第一个查完再直接从缓存拿。
        ReentrantLock lock = keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            // Double check：获取锁后再次检查本地缓存
            value = localCache.getIfPresent(key);
            if (value != null) {
                if ("NULL".equals(value)) {
                    return null;
                }
                return value;
            }

            // ========== 第四层：Redis分布式缓存 L2 ==========
            // 如果 Redis 有数据，拿到后回填本地缓存，下次就直接从本地拿了
            try (Jedis jedis = jedisPool.getResource()) {
                value = jedis.get(key);
                if (value != null) {
                    // 检查是否是空值标记
                    if ("NULL".equals(value)) {
                        System.out.println("[Cache] L2 hit but NULL marker: " + key);
                        // 空值也要缓存在本地，避免重复查询Redis
                        localCache.put(key, "NULL");
                        return null;
                    }
                    System.out.println("[Cache] L2 hit: " + key);
                    localCache.put(key, value);
                    return value;
                }
            }
            System.out.println("[Cache] L2 miss: " + key);

            // ========== 第五层：数据库（DataLoader） ==========
            //查到数据后写入 Redis 和本地缓存，下次就不用再查数据库了
            if (dataLoader != null) {
                value = dataLoader.load();
                if (value != null) {
                    // 写入缓存（防雪崩：随机过期时间）
//                    int baseExpire = 5;  // 改成5秒
//                    int randomOffset = random.nextInt(3);
                    int baseExpire = 300;
                    int randomOffset = random.nextInt(60);  // 0-60秒随机
                    int expireTime = baseExpire + randomOffset;

                    try (Jedis jedis = jedisPool.getResource()) {
                        jedis.setex(key, expireTime, value);
                    }
                    localCache.put(key, value);
                    bloomFilter.put(key);  // 加入布隆过滤器
                    System.out.println("[Cache] Loaded from DB and cached, expire: " + expireTime + "s");
                } else {
                    // 空值缓存（防穿透），过期时间短一些
                    try (Jedis jedis = jedisPool.getResource()) {
                        jedis.setex(key, 60, "NULL");
                    }
                    localCache.put(key, "NULL");
                    System.out.println("[Cache] NULL cached for key: " + key);
                }
            }
            return value;
        } finally {
            lock.unlock();
            keyLocks.remove(key);
        }
    }

    //缓存失效方法
    public void evict(String key) {
        localCache.invalidate(key);// 清除本地缓存
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key); // 删除Redis中的Key
        }
        System.out.println("[Cache] Evicted: " + key);
    }

    //获取本地缓存命中率
    public double getHitRate() {
        return localCache.stats().hitRate();
    }

    //添加Key到布隆过滤器
    public void addToBloomFilter(String key) {
        bloomFilter.put(key);
    }

    public void close() {
        jedisPool.close();
    }

    @FunctionalInterface
    public interface DataLoader {
        String load();
    }
}