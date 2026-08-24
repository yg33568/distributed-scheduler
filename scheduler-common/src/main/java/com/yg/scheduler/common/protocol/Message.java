package com.yg.scheduler.common.protocol;

import lombok.Data;

import static com.yg.scheduler.common.protocol.ProtocolConstants.*;

@Data
public class Message {
    /**
     * 魔数
     * 值为 0xCAFEBABE，用于快速识别是不是我的协议
     * */
    private int magicNumber;

    /** 协议版本 */
    private byte version;

    /**消息类型：1=请求  2=响应  3=心跳  100=注册* */
    private byte type;

    /** 消息状态   1=成功 2=失败*/
    private byte status;

    /**
     * 消息体长度
     * 单位字节，用于解码器判断是否收完整
     * */
    private int length;

    /** 消息体（JSON格式） */
    private byte[] body;

    //初始化魔数和版本为默认值
    public Message() {
        this.magicNumber = ProtocolConstants.MAGIC_NUMBER;
        this.version = ProtocolConstants.VERSION;
    }

    //静态工厂方法，创建一个"请求"类型的消息
    public static Message request(byte[] body) {
        Message msg = new Message();
        msg.setType(ProtocolConstants.TYPE_REQUEST);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(body);
        msg.setLength(body.length);
        return msg;
    }

    //静态工厂方法，创建一个"响应"类型的消息
    public static Message response(byte[] body) {
        Message msg = new Message();
        msg.setType(ProtocolConstants.TYPE_RESPONSE);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(body);
        msg.setLength(body.length);
        return msg;
    }

    //静态工厂方法，创建一个"心跳"消息（无消息体）
    public static Message heartbeat() {
        Message msg = new Message();
        msg.setType(ProtocolConstants.TYPE_HEARTBEAT);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(new byte[0]);
        msg.setLength(0);
        return msg;
    }

    //静态工厂方法，创建一个"注册"类型的消息
    public static Message register(byte[] body) {
        Message msg = new Message();
        msg.setType(TYPE_REGISTER);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(body);
        msg.setLength(body.length);
        return msg;
    }


    //静态工厂方法，创建一个"缓存迁移"消息
    public static Message cacheMigrate(byte[] body) {
        Message msg = new Message();
        msg.setType(TYPE_CACHE_MIGRATE);
        msg.setStatus(STATUS_SUCCESS);
        msg.setBody(body);
        msg.setLength(body.length);
        return msg;
    }
}