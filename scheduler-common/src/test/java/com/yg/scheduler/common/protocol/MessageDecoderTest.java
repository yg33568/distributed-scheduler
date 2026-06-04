package com.yg.scheduler.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessageDecoderTest {

    // 构建一个完整的消息
    private byte[] buildMessage(byte[] body) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(ProtocolConstants.MAGIC_NUMBER);
        buf.writeByte(ProtocolConstants.VERSION);
        buf.writeByte(ProtocolConstants.TYPE_REQUEST);
        buf.writeByte(ProtocolConstants.STATUS_SUCCESS);
        buf.writeInt(body.length);
        buf.writeBytes(body);
        byte[] result = new byte[buf.readableBytes()];
        buf.readBytes(result);
        return result;
    }

    @Test
    public void testNormalDecode() {
        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder());
        byte[] body = "hello".getBytes();
        byte[] msg = buildMessage(body);
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(msg);
        channel.writeInbound(buf);

        Message result = channel.readInbound();
        assertNotNull(result);
        assertEquals(ProtocolConstants.TYPE_REQUEST, result.getType());
        assertEquals("hello", new String(result.getBody()));
    }

    @Test
    public void testTwoMessagesTogether() {
        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder());
        byte[] body1 = "hello".getBytes();
        byte[] body2 = "world".getBytes();
        byte[] msg1 = buildMessage(body1);
        byte[] msg2 = buildMessage(body2);

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(msg1);
        buf.writeBytes(msg2);
        channel.writeInbound(buf);

        Message result1 = channel.readInbound();
        Message result2 = channel.readInbound();
        assertNotNull(result1);
        assertNotNull(result2);
        assertEquals("hello", new String(result1.getBody()));
        assertEquals("world", new String(result2.getBody()));
    }

    @Test
    public void testHalfPackage() {
        EmbeddedChannel channel = new EmbeddedChannel(new MessageDecoder());
        byte[] body = "hello".getBytes();
        byte[] msg = buildMessage(body);

        // 只发一半
        int half = msg.length / 2;
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(msg, 0, half);
        channel.writeInbound(buf);

        Message result = channel.readInbound();
        assertNull(result);  // 半包不应该解码出消息

        // 发剩余一半
        ByteBuf buf2 = Unpooled.buffer();
        buf2.writeBytes(msg, half, msg.length - half);
        channel.writeInbound(buf2);

        result = channel.readInbound();
        assertNotNull(result);
        assertEquals("hello", new String(result.getBody()));
    }
}