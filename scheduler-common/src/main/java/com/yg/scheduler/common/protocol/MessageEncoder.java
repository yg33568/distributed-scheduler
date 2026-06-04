package com.yg.scheduler.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

//编码器：把 Message 对象按协议格式写回 ByteBuf
public class MessageEncoder extends MessageToByteEncoder<Message> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
        // 写入魔数
        out.writeInt(msg.getMagicNumber());
        // 写入版本
        out.writeByte(msg.getVersion());
        // 写入消息类型
        out.writeByte(msg.getType());
        // 写入状态
        out.writeByte(msg.getStatus());
        // 写入消息体长度
        out.writeInt(msg.getLength());
        // 写入消息体
        if (msg.getBody() != null && msg.getBody().length > 0) {
            out.writeBytes(msg.getBody());
        }
    }
}