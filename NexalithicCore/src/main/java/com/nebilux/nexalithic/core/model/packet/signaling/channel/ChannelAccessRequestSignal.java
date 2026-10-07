package com.nebilux.nexalithic.core.model.packet.signaling.channel;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;

/**
 * 指定目标 Channel 类型的接入请求。
 *
 * @author Reonvia
 * @since 0.2.2
 */
public final class ChannelAccessRequestSignal extends ChannelAccessSignal {
    private static final int CONTENT_LENGTH = KIND_LENGTH;

    public ChannelAccessRequestSignal(NexalithicChannel.Kind kind) {
        super(Signal.ChannelAccess_Request, kind, CONTENT_LENGTH);
    }

    private ChannelAccessRequestSignal(LoopBuffer buffer, int contentLength) {
        super(Signal.ChannelAccess_Request, buffer, contentLength, CONTENT_LENGTH, CONTENT_LENGTH);
    }

    public static ChannelAccessRequestSignal fromBuffer(LoopBuffer buffer, int contentLength) {
        return new ChannelAccessRequestSignal(buffer, contentLength);
    }

    @Override
    protected void writePayload(LoopBuffer buffer) {}
}
