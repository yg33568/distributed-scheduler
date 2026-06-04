package com.yg.scheduler.common;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobContext implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 任务ID */
    private Long jobId;

    /** 任务名称 */
    private String jobName;

    /** 任务参数 */
    private String params;

    /** 分片总数 */
    private Integer shardingTotal;

    /** 当前分片项（0-based） */
    private Integer shardingItem;

    /** 超时时间（秒） */
    private Integer timeout;

    /** 唯一任务id */
    private String taskId;

    /** 重试次数 */
    private Integer retryCount;
}