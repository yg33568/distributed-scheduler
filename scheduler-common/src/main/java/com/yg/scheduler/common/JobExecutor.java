package com.yg.scheduler.common;

public interface JobExecutor {

    /**
     * 执行任务
     * @param context 任务上下文
     * @return 执行结果
     */
    ExecutionResult execute(JobContext context);
}