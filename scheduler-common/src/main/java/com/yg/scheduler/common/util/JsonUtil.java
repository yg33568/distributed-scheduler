package com.yg.scheduler.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 因为网络只能传输字节（0和1），不能直接传 Java对象。
 *必须先把它打包成字节，对方收到后再拆包还原。
 */
public class JsonUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    //把 Java对象变成字节数组
    public static String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException("序列化失败", e);
        }
    }

    //把字节数组变回 Java对象
    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException("反序列化失败", e);
        }
    }
}