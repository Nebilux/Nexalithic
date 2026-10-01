package com.nebilux.nexalithic.core.io.codec;

/**
 * 数据包帧
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class PacketFrame {
    public static final int FRAME_HEADER_LENGTH = Long.BYTES; // 8 Bytes
    public static final int MAX_PAYLOAD_LENGTH = 1024 * 16;  // 16 KB
    private static final int START_FLAG_SHIFT  = 63;
    private static final int PAYLOAD_LEN_SHIFT = 48; // 16(Reserved) + 32(ID)
    private static final int PAYLOAD_LEN_MASK  = 0x7FFF; // 15 bits

    /**
     * 打包元数据
     */
    public static long pack(int packetId, int payloadLen, boolean isStart) {
        long meta = 0;
        if (isStart) {
            meta |= (1L << START_FLAG_SHIFT);
        }
        meta |= (long) (payloadLen & PAYLOAD_LEN_MASK) << PAYLOAD_LEN_SHIFT;
        meta |= (packetId & 0xFFFFFFFFL);
        return meta;
    }

    /**
     * 解析 Packet ID
     */
    public static int parsePacketId(long header) {
        return (int) header;
    }

    /**
     * 解析 Payload Length
     */
    public static int parsePayloadLength(long header) {
        return (int) (header >>> PAYLOAD_LEN_SHIFT) & PAYLOAD_LEN_MASK;
    }

    /**
     * 是否为起始帧
     */
    public static boolean isStart(long header) {
        return header < 0;
    }
}
