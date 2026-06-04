package com.yg.scheduler.common;

import lombok.Data;
import java.io.Serializable;

@Data
public class ExecutionResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long jobId;
    private Boolean success;
    private String message;
    private Object data;
    private String taskId;

    public static ExecutionResult success(Long jobId,String taskId, Object data) {
        ExecutionResult result = new ExecutionResult();
        result.setJobId(jobId);
        result.setTaskId(taskId);
        result.setSuccess(true);
        result.setMessage("SUCCESS");
        result.setData(data);
        return result;
    }

    public static ExecutionResult failure(Long jobId, String taskId,String message) {
        ExecutionResult result = new ExecutionResult();
        result.setJobId(jobId);
        result.setTaskId(taskId);
        result.setSuccess(false);
        result.setMessage(message);
        return result;
    }
}