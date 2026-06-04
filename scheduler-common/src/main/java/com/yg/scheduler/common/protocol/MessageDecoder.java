package com.yg.scheduler.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/**
 * 解码器的工作：
 * 攒数据，直到够读头部
 * 从头部里拿到 Body 长度
 * 判断 Body 是否完整，不完整就等下次
 * Body 完整就切出来，交给业务处理
 */
public class MessageDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 检查可读字节数是否足够解析头部
        if (in.readableBytes() < ProtocolConstants.HEADER_LENGTH) {
            return; // 不够，等待更多数据
        }

        // 标记当前读位置，以便回滚
        in.markReaderIndex();

        // 读取魔数
        int magicNumber = in.readInt();
        if (magicNumber != ProtocolConstants.MAGIC_NUMBER) {
            // 魔数不匹配，不是我们的协议，关闭连接
            ctx.close();
            return;
        }

        // 读取版本
        byte version = in.readByte();
        // 读取消息类型
        byte type = in.readByte();
        // 读取状态
        byte status = in.readByte();
        // 读取消息体长度
        int length = in.readInt();

        // 检查消息体长度是否合法
        if (length < 0 || length > 1024 * 1024) { // 最大1MB
            ctx.close();
            return;
        }

        // 检查是否收到完整的消息体
        if (in.readableBytes() < length) {
            // 不够，重置读位置，等待更多数据
            in.resetReaderIndex();
            return;
        }

        // 读取消息体
        byte[] body = new byte[length];
        in.readBytes(body);

        // 构建Message对象
        Message message = new Message();
        message.setMagicNumber(magicNumber);
        message.setVersion(version);
        message.setType(type);
        message.setStatus(status);
        message.setLength(length);
        message.setBody(body);

        // 解码完成，传递给下一个Handler
        out.add(message);
    }
}