package com.yg.scheduler.common.protocol;

public class ProtocolConstants {

    /** 魔数，用于快速识别协议 */
    public static final int MAGIC_NUMBER = 0xCAFEBABE;

    /** 协议头长度（字节） */
    public static final int HEADER_LENGTH = 4 + 1 + 1 + 1 + 4; // 魔数4 + 版本1 + 类型1 + 状态1 + 长度4 = 11字节

    /** 协议版本 */
    public static final byte VERSION = 1;

    /** 消息类型：请求 */
    public static final byte TYPE_REQUEST = 1;

    /** 消息类型：响应 */
    public static final byte TYPE_RESPONSE = 2;

    /** 消息类型：心跳 */
    public static final byte TYPE_HEARTBEAT = 3;

    /** 消息状态：成功 */
    public static final byte STATUS_SUCCESS = 1;

    /** 消息状态：失败 */
    public static final byte STATUS_FAIL = 2;

    public static final byte TYPE_REGISTER = 100;
}