package com.nebilux.nexalithic.core.model.packet;

import com.nebilux.nexalithic.core.model.AbstractModel;

import java.nio.charset.StandardCharsets;

/**
 * 抽象包
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class AbstractPacket implements AbstractModel {
    public static final int MAX_PAYLOAD_COUNT = Byte.MAX_VALUE;
    public enum PacketType {
        /** 信令包 */ Signaling,
        /** 业务包 */ Business,
    }

    /**
     * 获取包类型
     *
     * @return {@link PacketType }
     */
    public abstract PacketType packetType();
    public final ModelType modelType() {
        return ModelType.Packet;
    }

    public static byte[] shortToBytes(short value) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) ((value >> 8) & 0xFF);
        bytes[1] = (byte) ((value) & 0xFF);
        return bytes;
    }
    public static byte[] intToBytes(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((value >> 24) & 0xFF);
        bytes[1] = (byte) ((value >> 16) & 0xFF);
        bytes[2] = (byte) ((value >> 8) & 0xFF);
        bytes[3] = (byte) ((value) & 0xFF);
        return bytes;
    }
    public static byte[] longToBytes(long value) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((value >> 56) & 0xFF);
        bytes[1] = (byte) ((value >> 48) & 0xFF);
        bytes[2] = (byte) ((value >> 40) & 0xFF);
        bytes[3] = (byte) ((value >> 32) & 0xFF);
        bytes[4] = (byte) ((value >> 24) & 0xFF);
        bytes[5] = (byte) ((value >> 16) & 0xFF);
        bytes[6] = (byte) ((value >> 8) & 0xFF);
        bytes[7] = (byte) ((value) & 0xFF);
        return bytes;
    }
    public static byte[] stringToBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static short bytesToShort(byte[] value) {
        return (short) (((value[0] & 0xFF) << 8) | (value[1] & 0xFF));
    }
    public static int bytesToInt(byte[] value) {
        return ((value[0] & 0xFF) << 24) |
                ((value[1] & 0xFF) << 16) |
                ((value[2] & 0xFF) <<  8) |
                ((value[3] & 0xFF));
    }
    public static long bytesToLong(byte[] value) {
        return ((long) (value[0] & 0xFF) << 56) |
                ((long) (value[1] & 0xFF) << 48) |
                ((long) (value[2] & 0xFF) << 40) |
                ((long) (value[3] & 0xFF) << 32) |
                ((long) (value[4] & 0xFF) << 24) |
                ((long) (value[5] & 0xFF) << 16) |
                ((long) (value[6] & 0xFF) << 8)  |
                ((long) (value[7] & 0xFF));
    }
    public static String bytesToString(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
