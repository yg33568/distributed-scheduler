package com.yg.scheduler.common.protocol;

import lombok.Data;

import static com.yg.scheduler.common.protocol.ProtocolConstants.STATUS_SUCCESS;
import static com.yg.scheduler.common.protocol.ProtocolConstants.TYPE_REGISTER;

@Data
public class Message {
    /** 魔数 */
    private int magicNumber;

    /** 协议版本 */
    private byte version;

    /** 消息类型：请求/响应/心跳 */
    private byte type;

    /** 消息状态 */
    private byte status;

    /** 消息体长度 */
    private int length;

    /** 消息体（JSON格式） */
    private byte[] body;

    public Message() {
        this.magicNumber = ProtocolConstants.MAGIC_NUMBER;
        this.version = ProtocolConstants.VERSION;
    }

    public static Message request(byte[] body) {
        Message msg = new Message();
        msg.setType(ProtocolConstants.TYPE_REQUEST);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(body);
        msg.setLength(body.length);
        return msg;
    }

    public static Message response(byte[] body) {
        Message msg = new Message();
        msg.setType(ProtocolConstants.TYPE_RESPONSE);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(body);
        msg.setLength(body.length);
        return msg;
    }

    public static Message heartbeat() {
        Message msg = new Message();
        msg.setType(ProtocolConstants.TYPE_HEARTBEAT);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(new byte[0]);
        msg.setLength(0);
        return msg;
    }

    public static Message register(byte[] body) {
        Message msg = new Message();
        msg.setType(TYPE_REGISTER);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(body);
        msg.setLength(body.length);
        return msg;
    }

    public static final byte TYPE_CACHE_MIGRATE = 101;

    public static Message cacheMigrate(byte[] body) {
        Message msg = new Message();
        msg.setType(TYPE_CACHE_MIGRATE);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(body);
        msg.setLength(body.length);
        return msg;
    }
}