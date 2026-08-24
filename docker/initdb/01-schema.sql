-- =====================================================================
-- 分布式任务调度系统 - 建表脚本
-- 挂载到 MySQL 容器的 /docker-entrypoint-initdb.d/，首次启动自动执行
-- 列名与 JobDao.save() / updateStatus() 严格一致
-- =====================================================================

CREATE DATABASE IF NOT EXISTS scheduler DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE scheduler;

CREATE TABLE IF NOT EXISTS schedule_job (
    job_id          BIGINT       NOT NULL COMMENT '任务ID',
    task_id         VARCHAR(64)  NOT NULL COMMENT '唯一任务标识(幂等键)',
    job_name        VARCHAR(128) NOT NULL COMMENT '任务名称',
    params          TEXT                  COMMENT '任务参数(JSON)',
    sharding_total  INT          DEFAULT 1  COMMENT '分片总数',
    sharding_item   INT          DEFAULT 0  COMMENT '当前分片项',
    timeout         INT          DEFAULT 30 COMMENT '超时秒数',
    target_worker   VARCHAR(128)          COMMENT '派发目标worker',
    status          VARCHAR(16)  DEFAULT 'PENDING' COMMENT 'PENDING/RUNNING/SUCCESS/FAILED/TIMEOUT',
    retry_count     INT          DEFAULT 0  COMMENT '重试次数',
    error_msg       VARCHAR(512)          COMMENT '失败/超时原因',
    create_time     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (job_id),
    UNIQUE KEY uk_task_id (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='调度任务表';
