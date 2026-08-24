package com.yg.scheduler.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.util.List;

// CacheMigrationMessage是一个消息对象
// 用于在调度中心和执行器之间传递缓存迁移的指令。
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheMigrationMessage implements Serializable {
    private List<String> hotKeys; // 需要迁移的热点Key列表
}