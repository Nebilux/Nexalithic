package com.nebilux.nexalithic.core.model.packet.signaling;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.channel.ChannelAccessRequestSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.channel.ChannelAccessResponseSignal;

import java.lang.reflect.Field;

/**
 * 信令包
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class SignalingPacket extends AbstractPacket {
    public static class Signal {
        public static final byte HeartBeat = 0x0;
        public static final byte BusinessChannelRate = -0x3;
        public static final byte ChannelAccess_Request = -0x4;
        public static final byte ChannelAccess_Response = 0x4;
    }
    public static final int HEADER_LENGTH = Byte.BYTES + Short.BYTES;
    public static final int MAX_PACKET_LENGTH = 1024 * 4;

    protected static final String[] NAMES = new String[256];
    protected final byte signal;
    protected short length;

    static {
        for (Field field : Signal.class.getDeclaredFields()) {
            if (field.getType() == byte.class) {
                try {
                    byte value = field.getByte(null);
                    NAMES[value & 0xFF] = field.getName();
                } catch (IllegalAccessException exception) {
                    throw new ExceptionInInitializerError(exception);
                }
            }
        }
    }

    protected SignalingPacket(byte signal) {
        this.signal = signal;
    }

    public final boolean toBuffer(LoopBuffer buffer) {
        if (buffer.writableBytes() < getTotalSize()) {
            return false;
        }
        buffer.unsafePut(signal);
        buffer.unsafePut(length);
        onToBuffer(buffer);
        return true;
    }
    public static SignalingPacket fromBuffer(LoopBuffer buffer) {
        buffer.markHead();
        byte signal = buffer.unsafeGetByte();
        int length = buffer.unsafeGetShort();
        if (length < 0 || length + HEADER_LENGTH > MAX_PACKET_LENGTH) {
            throw new IllegalArgumentException("Signaling packet length exceeds maximum: " + length);
        }
        if (buffer.readableBytes() < length) {
            buffer.resetHead();
            return null;
        }
        return switch (signal) {
            case Signal.BusinessChannelRate -> new ScalarSignal(signal, buffer.unsafeGetLong());
            case Signal.ChannelAccess_Request -> ChannelAccessRequestSignal.fromBuffer(buffer, length);
            case Signal.ChannelAccess_Response -> ChannelAccessResponseSignal.fromBuffer(buffer, length);
            default -> {
                if (length == 0) {
                    BareSignal bare = BareSignal.find(signal);
                    yield bare != null ? bare : new RawSignal(signal, null);
                } else {
                    byte[] content = new byte[length];
                    buffer.unsafeGetBytes(content, length);
                    yield new RawSignal(signal, content);
                }
            }
        };
    }

    public byte getSignal() {
        return signal;
    }
    public int getTotalSize() {
        return HEADER_LENGTH + getLength();
    }

    protected abstract void onToBuffer(LoopBuffer buffer);
    public abstract short getLength();

    public static String toName(byte signal) {
        String name = NAMES[signal & 0xFF];
        return name != null ? name : "UNKNOWN_SIGNAL(" + String.format("0x%02X", signal) + ")";
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " -> Signal: " + toName(signal);
    }

    @Override
    public PacketType packetType() {
        return PacketType.Signaling;
    }
}
