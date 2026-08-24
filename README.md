# 分布式任务调度系统 (Distributed Scheduler)

一个**从零手写**的分布式任务调度系统：调度中心通过一致性哈希把分片任务路由到多个执行器，执行器用多级缓存扛热点，全程 Netty 自定义 TCP 协议通信，调度中心支持 **ZooKeeper 选主的主备高可用集群**，可 **Docker 一键部署**。

> 不依赖任何现成调度框架（如 XXL-Job / ElasticJob / Quartz），核心全部手写，适合深挖原理的面试项目。

---

## 技术栈

| 层 | 技术 |
|---|---|
| 语言 | **Java 17**（Maven 多模块） |
| 通信 | **Netty** 自定义 TCP 协议（魔数 0xCAFEBABE，粘包/半包处理） |
| 调度 | 一致性哈希（MD5 + TreeMap + 150 虚拟节点）分片路由 |
| 高可用 | **Apache Curator + ZooKeeper**：LeaderLatch 选主、临时节点服务发现、主备切换 |
| 缓存 | **Caffeine** L1 + **Redis** L2 + Guava **布隆过滤器**（防穿透/防击穿/防雪崩） |
| 持久化 | **MySQL** + HikariCP（任务表，幂等唯一键） |
| 构建/部署 | Maven 多模块 + maven-shade fat-jar + **Docker Compose** |

---

## 架构图

```
                         ┌──────────────────────────┐
                         │        ZooKeeper         │
                         │  /scheduler/leader-latch │  ← 主备选举
                         │  /scheduler/leader       │  ← 当前leader地址(服务发现)
                         └────────────┬─────────────┘
                                      │ 选主 / 发现
        ┌─────────────────────────────┼───────────────────────────┐
        │                             │                           │
┌───────▼───────┐           ┌─────────▼─────────┐       ┌─────────▼─────────┐
│  scheduler-a  │   Leader  │  scheduler-b(备)  │  ...  │   更多 master     │
│   (Leader)    │◄──────────┤    standby        │       │     standby       │
│  TCP:8080     │  ZK选主后  │    (待命接管)      │       │                   │
│  Admin:8081   │           │    Admin:8081     │       │                   │
└───┬───────┬───┘           └───────────────────┘       └───────────────────┘
    │       │ 一致性哈希分片路由 (taskId → worker)
    │       │
┌───▼───────▼───┐   ┌───────────▼───────────┐
│  worker-a     │   │  worker-b            │
│  多级缓存 L1L2 │   │  多级缓存 L1L2        │
└───────┬───────┘   └───────────┬───────────┘
        │                       │
   ┌────▼────┐            ┌─────▼────┐
   │  MySQL  │            │  Redis   │   (L2 共享)
   └─────────┘            └──────────┘
```

**关键流程**

1. **注册**：worker 启动 → 从 ZK 读取当前 leader 地址 → TCP 长连接 → 发注册消息 → leader 回 `registerAck` → 加入一致性哈希环。
2. **分片调度**：leader 每 30s 生成 10 个分片任务，`router.route(taskId)` 路由到对应 worker，登记内存状态 + 设超时定时器 + 落库。
3. **执行**：worker 收到任务 → 幂等检查（`executedTasks`/`executingTasks`）→ 多级缓存读数据 → 回结果。
4. **故障转移**：worker 下线 → leader 把未完成任务放回重试队列、把热点 key 按一致性哈希**后继节点**迁移过去、重建哈希环。
5. **主备切换**：leader 挂 → ZK 选举 → 备机接管 → 从 DB reload 未完成任务重派（worker 幂等兜底）→ worker 自动重连新 leader。

---

## 核心特性

### 1. 手写 Netty 自定义协议
- 帧头 11 字节：`魔数(4) + 版本(1) + 类型(1) + 状态(1) + 长度(4)`
- 类型：`request/response/heartbeat/register/cacheMigrate/registerAck`
- 解码器处理**粘包/半包**（`ByteToMessageDecoder` + `markReaderIndex` 回滚）
- 单元测试覆盖：正常解码 / 两包连发 / 半包分次到达

### 2. 一致性哈希分片路由
- MD5 哈希 + TreeMap 环 + 每节点 150 虚拟节点，节点增减只影响相邻分片
- worker 下线时，其热点 key 按哈希环求**正确后继者**迁移（非任意节点）

### 3. 多级缓存（防穿透 / 防击穿 / 防雪崩）
```
布隆过滤器(已知存在集合, 需预热) → L1 Caffeine → 条带锁(Striped, 防击穿) → L2 Redis → DB
```
- **防穿透**：布隆过滤器 + 空值(NULL)短缓存
- **防击穿**：Guava `Striped.lazyWeakLock` 固定条带锁（消除 per-key Map 的 remove 竞态）
- **防雪崩**：写 Redis 随机过期时间
- **降级**：Redis 故障降级到 DB，不阻断读取
- worker 间缓存一致性：预留 `CacheMessage`（EVICT/UPDATE）广播（见已知限制）

### 4. 主备高可用（Master 集群）
- **Curator LeaderLatch** 在 `/scheduler/leader-latch` 选主；leader 才启动 TCP + 调度
- leader 地址写入 ZK 临时节点 `/scheduler/leader`，worker 通过 `PathChildrenCache` 服务发现
- **故障恢复**：新 leader 从 MySQL reload `PENDING/RUNNING` 任务重派，worker 端双集合幂等兜底
- 防脑裂：被降级节点 `shutdown()` 停止调度器与 TCP，避免旧 leader 继续调度

### 5. Worker 自动重连
- leader 切换 → worker 检测断线 → 指数退避+抖动重连 → 重新注册
- workerId 持久化（配置 > 文件 > 生成），重启不漂移，哈希亲和稳定

---

## 目录结构

```
distributed-scheduler/
├── scheduler-common/          # 协议、消息对象、配置、ZK 工具
│   └── .../protocol/          #   Message/Decoder/Encoder/Constants
│   └── .../config/AppConfig   #   环境变量 > properties > 默认值
│   └── .../zk/ZkClients       #   Curator 客户端工厂
├── scheduler-core/            # 调度中心
│   ├── SchedulerServer        #   TCP 服务 + 分片/重试调度 + 派发
│   ├── MasterLifecycle        #   ZK 选主 + 主备切换
│   ├── JobRecovery            #   新 leader 任务恢复
│   ├── ConsistentHashRouter   #   一致性哈希
│   ├── JobDao                 #   MySQL 持久化
│   ├── ServerHandler          #   注册/心跳/结果处理
│   └── AdminServer/Handler    #   HTTP 控制台 /status /leader /workers
├── scheduler-worker/          # 执行器
│   ├── SchedulerClient        #   重连循环 + 心跳
│   ├── LeaderDiscovery        #   ZK 发现 leader
│   ├── ClientHandler          #   任务执行 + 幂等 + 缓存迁移
│   ├── CacheService           #   多级缓存
│   └── WorkerRuntime          #   进程内共享运行时
├── docker/
│   └── initdb/01-schema.sql   # MySQL 建表脚本
├── scripts/run-local.*        # 本地手动启动兜底
└── docker-compose.yml         # 一键部署
```

---

## 快速开始（Docker，推荐）

前置：Docker Desktop（WSL2）。

```bash
docker compose up -d --build
docker compose ps          # 全部 healthy 后继续
```

**验证**

```bash
# 主备状态（scheduler-a 应为 leader，scheduler-b standby）
curl localhost:18081/status
curl localhost:18082/status

# worker 列表 + 任务增长
curl localhost:18081/workers
curl localhost:18081/jobs/pending
```

**演示故障切换（HA）**

```bash
# 杀掉 leader
docker compose stop scheduler-a

# 观察 scheduler-b 日志 "BECAME LEADER"，worker 日志 "Leader discovered/changed"
docker compose logs -f scheduler-b

# scheduler-b 接管，/workers 恢复两个节点，任务持续 SUCCESS
curl localhost:18082/status
curl localhost:18082/workers

# 让 scheduler-a 回归 standby
docker compose start scheduler-a
```

**演示 worker 下线（任务转移 + 缓存迁移）**

```bash
docker compose stop worker-a
# leader 日志出现 Re-queued task ... + [Migration] Migrated N keys from worker-a to worker-b
docker compose logs -f scheduler-a
```

---

## 本地手动启动（无 Docker）

1. 启动本机 ZooKeeper(2181)、MySQL(3306)、Redis(6379)；在 MySQL 执行 `docker/initdb/01-schema.sql`
2. 打包：`mvn -DskipTests package`
3. 运行：
   - **Windows**：`scripts\run-local.bat`
   - **bash/WSL**：`scripts/run-local.sh`
4. 或手动 `java -jar scheduler-core/target/scheduler-core-1.0-SNAPSHOT-all.jar`（可用 `SCHEDULER_PORT`/`WORKER_ID` 等环境变量覆盖）

---

## 配置说明（环境变量 > application.properties > 默认值）

| 变量 | 说明 | 默认 |
|---|---|---|
| `ZK_CONNECT` | ZooKeeper 地址 | `localhost:2181` |
| `DB_JDBC_URL` / `DB_USERNAME` / `DB_PASSWORD` | MySQL | `localhost:3306/scheduler` root/123456 |
| `REDIS_HOST` / `REDIS_PORT` | Redis | `localhost:6379` |
| `SCHEDULER_PORT` / `SCHEDULER_ADMIN_PORT` | master TCP / admin | `8080` / `8081` |
| `SCHEDULER_ADVERTISE_HOST` | leader 对外广告地址（容器里填服务名） | `localhost` |
| `WORKER_ID` | worker 标识（不填则持久化生成） | 自动 |

---

## 设计要点（面试可深挖）

- **一致性哈希的虚拟节点**为什么要 150 个？→ 减少节点增减时的数据倾斜。
- **leader 切换任务不丢？** → 任务先落库（PENDING），新 leader `findPendingJobs()` 重派；worker 端 `executedTasks`（已完成）/`executingTasks`（执行中）双集合保证**至少一次执行（at-least-once）**。
- **防击穿为什么用 Striped 锁？** → per-key Map + `unlock 后 remove` 有竞态；固定条带锁无 map 无竞态。
- **布隆过滤器为什么必须先预热？** → 它是"已知存在集合"的门禁，不预热会误杀真实 key；生产从数据集批量灌入。
- **主备 vs 双活？** → 本实现选主备：单 leader 调度逻辑简单、无分布式锁开销；代价是待命节点资源闲置。

---

## 已知限制（真实项目都会诚实说明）

- **单 ZK 无法严格仲裁脑裂**：LeaderLatch 依赖会话超时，极端网络分区下可能短暂双 leader，靠 worker 幂等兜底为 at-least-once。生产需 3 节点 ZK ensemble + 数据层 fencing。
- **worker 重启丢幂等集**：`executedTasks` 在内存，重启后可能重复执行（at-least-once，对幂等任务无害）。可换 Caffeine 有界缓存。
- **调度中心为单 leader 主备**，未实现双活/水平扩展调度吞吐（可扩展点）。
- **无 CRON 表达式调度**：当前为固定分片演示任务（ShardTask），真实任务注册/CRON 是下一阶段扩展点。
- **worker 间缓存一致性**：`CacheMessage` 广播已预留未接线。

---

## 单元测试

```bash
mvn test
# scheduler-common/protocol/MessageDecoderTest：正常/粘包/半包
```

---

*项目用途：分布式系统原理实践 + 求职面试展示。*
