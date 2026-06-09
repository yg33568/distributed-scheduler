package com.yg.scheduler.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheMigrationMessage implements Serializable {
    private List<String> hotKeys;
}