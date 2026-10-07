package com.nebilux.nexalithic.core.model.packet.signaling;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;

import java.nio.ByteBuffer;

/**
 * 标量信号
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ScalarSignal extends SignalingPacket {
    public static final int LENGTH = Long.BYTES;
    private final long value;

    public ScalarSignal(byte signal, long value) {
        super(signal);
        this.value = value;
        length = LENGTH;
    }
    public ScalarSignal(byte signal, ByteBuffer buffer) {
        this(signal, buffer.getLong());
    }
    public ScalarSignal(byte signal, LoopBuffer buffer) {
        this(signal, buffer.getLong());
    }

    public static ScalarSignal ofByte(byte signal, byte value) {
        return new ScalarSignal(signal, value);
    }
    public static ScalarSignal ofShort(byte signal, short value) {
        return new ScalarSignal(signal, value);
    }
    public static ScalarSignal ofInt(byte signal, int value) {
        return new ScalarSignal(signal, value);
    }
    public static ScalarSignal ofLong(byte signal, long value) {
        return new ScalarSignal(signal, value);
    }
    public static ScalarSignal ofFloat(byte signal, float value) {
        return new ScalarSignal(signal, Float.floatToIntBits(value));
    }
    public static ScalarSignal ofDouble(byte signal, double value) {
        return new ScalarSignal(signal, Double.doubleToLongBits(value));
    }
    public static ScalarSignal ofBoolean(byte signal, boolean value) {
        return new ScalarSignal(signal, value ? 1 : 0);
    }

    public byte asByte() {
        return (byte) value;
    }
    public short asShort() {
        return (short) value;
    }
    public int asInt() {
        return (int) value;
    }
    public long asLong() {
        return value;
    }
    public float asFloat() {
        return Float.intBitsToFloat(asInt());
    }
    public double asDouble() {
        return Double.longBitsToDouble(asLong());
    }
    public boolean asBoolean() {
        return asInt() != 0;
    }

    @Override
    protected void onToBuffer(LoopBuffer buffer) {
        buffer.put(value);
    }

    @Override
    public short getLength() {
        return Long.BYTES;
    }

    @Override
    public String toString() {
        return super.toString() + ", Value: " + value;
    }
}
