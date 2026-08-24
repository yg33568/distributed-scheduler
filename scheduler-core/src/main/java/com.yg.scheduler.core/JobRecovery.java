package com.yg.scheduler.core;

import com.yg.scheduler.common.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 故障恢复：新的 leader 接管后，从 MySQL 恢复所有 PENDING/RUNNING 任务并重新派发。
 *
 * 为什么可靠：
 *  - 派发走统一入口 dispatchJob()，目标不在线会自动进重试队列，等 worker 重连后重发
 *  - worker 端幂等（executedTasks 已完成 / executingTasks 执行中）兜底，避免重复执行
 *  - 任务超时定时器由 dispatchJob 统一重设
 *
 * 语义：at-least-once（可能重复执行，但绝不丢失）。
 */
public class JobRecovery {

    private static final Logger log = LoggerFactory.getLogger(JobRecovery.class);

    private final SchedulerServer server;

    public JobRecovery(SchedulerServer server) {
        this.server = server;
    }

    public void recover() {
        List<JobContext> pending = JobDao.getInstance().findPendingJobs();
        int dispatched = 0;
        for (JobContext job : pending) {
            // 重置重试计数：本次故障不消耗业务重试次数
            job.setRetryCount(0);
            if (server.dispatchJob(job)) {
                dispatched++;
            }
        }
        log.info("[Recovery] Reloaded {} pending/running jobs from DB, {} dispatched", pending.size(), dispatched);
    }
}
