# 分布式任务调度系统

## 项目简介

从零开发的轻量级分布式任务调度框架，支持任务分片、路由、故障转移、超时重试、多级缓存等核心能力。

调度中心负责任务分发和调度，执行器负责任务执行。两者通过自定义二进制协议通信，支持动态扩缩容和节点故障自动恢复。

## 技术栈

| 分类 | 技术 | 版本 |
| --- | --- | --- |
| 语言 | Java | 17 |
| 网络通信 | Netty | 4.1.100.Final |
| 本地缓存 | Caffeine | 3.1.8 |
| 分布式缓存 | Redis / Jedis | 4.4.3 |
| 数据库 | MySQL / HikariCP | 8.0 |
| 布隆过滤器 | Guava | 33.0.0 |
| 构建工具 | Maven | - |

## 系统架构

本系统分为三个核心模块：

**1. scheduler-common（公共模块）**
- 定义私有协议（魔数、版本、类型、状态、长度、Body）
- 提供编解码器（MessageEncoder / MessageDecoder）
- 定义核心实体（JobContext、ExecutionResult、WorkerInfo）

**2. scheduler-core（调度中心）**
- 维护执行器注册表
- 一致性Hash分片路由
- 心跳检测与故障转移
- 任务持久化与超时重试
- 缓存迁移服务
- HTTP管理接口

**3. scheduler-worker（执行器）**
- 注册与心跳
- 任务执行
- 多级缓存（Caffeine + Redis）
- 缓存防护（穿透、击穿、雪崩）
- 分片感知预热


## 核心功能

### 1. 网络通信层
- 基于Netty实现调度中心与执行器长连接
- 自定义二进制协议（魔数 + 版本 + 类型 + 状态 + 长度 + Body）
- 通过长度字段解码器彻底解决TCP粘包/半包问题
- 主从Reactor线程模型（Boss Group + Worker Group）
- 单元测试覆盖正常/粘包/半包三种场景

### 2. 服务治理
- 执行器启动时自动注册到调度中心
- 心跳检测（30秒发送，60秒超时剔除）
- 节点宕机时，未完成任务自动重新分配给其他执行器
- HTTP管理接口（/workers、/jobs/pending）

### 3. 分片调度
- 一致性Hash算法（150个虚拟节点）实现任务分片路由
- 支持任务分片（shardingTotal + shardingItem）
- 定时调度（30秒一轮）
- 节点动态扩缩容时自动重建Hash环

### 4. 任务可靠性
- MySQL持久化存储任务信息
- 任务超时控制（timeoutExecutor + ScheduledFuture）
- 失败自动重试（最多3次，指数退避）
- 数据库唯一索引保证任务幂等

### 5. 多级缓存（Caffeine + Redis）
- L1本地缓存（Caffeine，maxSize=10000，expire=60s）
- L2分布式缓存（Redis，expire=300s + 随机偏移）
- 布隆过滤器 + 空值缓存防穿透
- 互斥锁 + 双重检查防击穿
- 随机TTL防雪崩
- 分片感知预热：调度中心提前告知可能用到的数据key
- 故障缓存迁移：节点宕机时，热点key自动迁移到其他节点

## 项目结构
```
distributed-scheduler/
├── scheduler-common/ # 公共模块
│ ├── protocol/ # 私有协议编解码器
│ ├── JobContext.java # 任务上下文
│ ├── ExecutionResult.java # 执行结果
│ └── WorkerInfo.java # 执行器信息
├── scheduler-core/ # 调度中心
│ ├── SchedulerServer.java # 调度中心启动类
│ ├── ServerHandler.java # 业务处理器
│ ├── ConsistentHashRouter.java # 一致性Hash路由
│ ├── CacheMigrationService.java # 缓存迁移服务
│ ├── JobDao.java # 数据持久化
│ └── AdminServer.java # 管理接口
└── scheduler-worker/ # 执行器
├── SchedulerClient.java # 执行器启动类
├── ClientHandler.java # 业务处理器
├── CacheConfig.java # Caffeine配置
└── CacheService.java # 多级缓存服务
```

## 快速开始

### 环境要求
- JDK 17+
- MySQL 8.0+
- Redis 5.0+

### 1. 创建数据库

```sql
CREATE DATABASE scheduler;
USE scheduler;

CREATE TABLE schedule_job (
    job_id BIGINT PRIMARY KEY,
    task_id VARCHAR(128) UNIQUE,
    job_name VARCHAR(255),
    params TEXT,
    sharding_total INT,
    sharding_item INT,
    timeout INT DEFAULT 30,
    target_worker VARCHAR(255),
    status VARCHAR(20),
    retry_count INT DEFAULT 0,
    error_msg TEXT,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```
### 2. 修改配置
修改 scheduler-core/JobDao.java 中的数据库连接：
```
config.setJdbcUrl("jdbc:mysql://localhost:3306/scheduler?useSSL=false");
config.setUsername("root");
config.setPassword("your_password");
```
修改 scheduler-worker/CacheService.java 中的Redis地址：
```
this.jedisPool = new JedisPool(poolConfig, "localhost", 6379);
```
### 3. 编译打包
```
mvn clean package
```
### 4. 启动调度中心
```
java -cp scheduler-core/target/scheduler-core-1.0-SNAPSHOT.jar com.yg.scheduler.core.SchedulerServer
```
### 5. 启动执行器（可启动多个）
```
java -cp scheduler-worker/target/scheduler-worker-1.0-SNAPSHOT.jar com.yg.scheduler.worker.SchedulerClient
```
### 6. 查看管理接口
浏览器访问：
```
http://localhost:8081/workers - 查看在线执行器

http://localhost:8081/jobs/pending - 查看待处理任务
```

运行效果
调度中心日志
```
SchedulerServer started on port 8080
Worker registered: worker-xxx, total workers: 3
[Sharding] Start, total shards: 10
[Sharding] Shard 0 -> worker-xxx
[Sharding] Shard 1 -> worker-xxx
...
[Sharding] End
[Migration] Migrated 10 keys from worker-xxx to worker-yyy
```
执行器日志
```
Connected to scheduler: 127.0.0.1:8080
Registered with workerId: worker-xxx
[Preload] Preloaded 10 keys
[Cache] L1 hit: user:100
Executing job: ShardTask, shard: 1
Result sent: {"success":true}
```
核心亮点
底层网络能力：从零实现Netty通信、自定义二进制协议、解决TCP粘包/半包
分布式调度：一致性Hash分片、故障转移、任务重分配
多级缓存：Caffeine + Redis + 三重防护（穿透/击穿/雪崩）
智能预热：分片感知预热 + 故障缓存迁移
高可用：心跳检测、节点剔除、任务转移、缓存迁移

后续规划
时间轮调度器（替代ScheduledExecutorService）
多种路由策略（轮询、随机、加权）
Redis Pub/Sub缓存一致性兜底
降级熔断机制
调度中心集群（Raft选主）

作者：yg33568
