package com.yg.scheduler.common;

import lombok.Data;
import java.io.Serializable;

// ExecutionResult 是执行结果对象
// 是执行器完成任务后返回给调度中心的“回执单”
@Data
public class ExecutionResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long jobId; //任务ID
    private Boolean success; //是否成功
    private String message; //结果描述
    private Object data; //返回数据
    private String taskId; //幂等键

    //静态方法，创建成功结果
    public static ExecutionResult success(Long jobId,String taskId, Object data) {
        ExecutionResult result = new ExecutionResult();
        result.setJobId(jobId);
        result.setTaskId(taskId);
        result.setSuccess(true);
        result.setMessage("SUCCESS");
        result.setData(data);
        return result;
    }

    //静态方法，创建失败结果
    public static ExecutionResult failure(Long jobId, String taskId,String message) {
        ExecutionResult result = new ExecutionResult();
        result.setJobId(jobId);
        result.setTaskId(taskId);
        result.setSuccess(false);
        result.setMessage(message);
        return result;
    }
}