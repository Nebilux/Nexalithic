package com.nebilux.nexalithic.core.model.packet.signaling;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;

/**
 * 裸信号
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/11
 * @version 1.0.0
 */
public class BareSignal extends SignalingPacket {
    public static final BareSignal HeartBeat = new BareSignal(Signal.HeartBeat);
    public static final BareSignal BusinessChannelToken_Request = new BareSignal(Signal.BusinessChannelToken_Request);
    public static final BareSignal BusinessChannelPort_Request = new BareSignal(Signal.BusinessChannelPort_Request);

    private static final BareSignal[] LOOKUP = new BareSignal[256];
    static {
        register(HeartBeat);
        register(BusinessChannelToken_Request);
        register(BusinessChannelPort_Request);
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
    public byte[] getContent() {
        return null;
    }

    @Override
    public short getContentLength() {
        return 0;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "-> Signal: " + toName(signal);
    }
}
