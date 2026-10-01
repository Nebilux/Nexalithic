package com.nebilux.nexalithic.core.model.packet.signaling;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.session.SessionKey;

import java.nio.ByteBuffer;

/**
 * 令牌信号
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class TokenSignal extends SignalingPacket {
    public static final int LENGTH = SessionKey.LENGTH;
    private final long keyHigh, keyLow;

    public TokenSignal(SessionKey key) {
        super(Signal.BusinessChannelToken_Response);
        keyHigh = key.high();
        keyLow = key.low();
        length = LENGTH;
    }
    public TokenSignal(LoopBuffer buffer) {
        super(Signal.BusinessChannelToken_Response);
        keyHigh = buffer.unsafeGetLong();
        keyLow = buffer.unsafeGetLong();
        length = LENGTH;
    }

    public SessionKey toSessionKey(SessionKey.Mutable sessionKey) {
        return sessionKey.wrap(keyHigh, keyLow);
    }
    public SessionKey getSessionKey() {
        return new SessionKey.Immutable(keyHigh, keyLow);
    }
    public long getKeyHigh() {
        return keyHigh;
    }
    public long getKeyLow() {
        return keyLow;
    }

    @Override
    protected void onToBuffer(LoopBuffer buffer) {
        buffer.unsafePut(keyHigh);
        buffer.unsafePut(keyLow);
    }

    @Override
    public byte[] getContent() {
        ByteBuffer buf = ByteBuffer.allocate(LENGTH);
        buf.putLong(keyHigh);
        buf.putLong(keyLow);
        return buf.array();
    }

    @Override
    public short getContentLength() {
        return LENGTH;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
