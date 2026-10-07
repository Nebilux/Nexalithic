package com.nebilux.nexalithic.core.model.packet.signaling;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
/**
 * 裸信号
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BareSignal extends SignalingPacket {
    public static final BareSignal HeartBeat = new BareSignal(Signal.HeartBeat);

    private static final BareSignal[] LOOKUP = new BareSignal[256];
    static {
        register(HeartBeat);
    }

    private static void register(BareSignal instance) {
        LOOKUP[instance.getSignal() & 0xFF] = instance;
    }
    private BareSignal(byte signal) {
        super(signal);
        length = 0;
    }

    /**
     * 自动查找单例
     * @return 如果不是 BareSignal 类型则返回 null
     */
    public static BareSignal find(byte signal) {
        return LOOKUP[signal & 0xFF];
    }

    @Override
    protected void onToBuffer(LoopBuffer buffer) {
    }

    @Override
    public short getLength() {
        return 0;
    }
}
