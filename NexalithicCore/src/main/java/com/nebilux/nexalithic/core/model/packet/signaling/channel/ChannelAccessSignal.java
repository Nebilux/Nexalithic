package com.nebilux.nexalithic.core.model.packet.signaling.channel;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;

import java.util.Objects;

/**
 * Channel 接入信号的公共基类。
 *
 * <p>统一管理稳定的 Channel Kind 协议编号、内容长度和序列化模板。子类只负责 Kind
 * 之后的专属载荷，避免请求与响应分别实现相同的 Kind 编解码逻辑。</p>
 *
 * @author Reonvia
 * @since 0.2.2
 */
public abstract sealed class ChannelAccessSignal extends SignalingPacket permits ChannelAccessRequestSignal, ChannelAccessResponseSignal {
    protected static final int KIND_LENGTH = Byte.BYTES;

    private final NexalithicChannel.Kind kind;

    protected ChannelAccessSignal(byte signal, NexalithicChannel.Kind kind, int contentLength) {
        super(signal);
        validateLength(contentLength, KIND_LENGTH, MAX_PACKET_LENGTH - HEADER_LENGTH);
        this.kind = Objects.requireNonNull(kind, "kind");
        this.length = (short) contentLength;
    }

    protected ChannelAccessSignal(byte signal, LoopBuffer buffer, int contentLength, int minimumLength, int maximumLength) {
        super(signal);
        Objects.requireNonNull(buffer, "buffer");
        validateLength(contentLength, minimumLength, maximumLength);
        this.kind = NexalithicChannel.Kind.fromCode(buffer.unsafeGetByte());
        this.length = (short) contentLength;
    }

    public final NexalithicChannel.Kind getKind() {
        return kind;
    }

    @Override
    protected final void onToBuffer(LoopBuffer buffer) {
        buffer.unsafePut(kind.code());
        writePayload(buffer);
    }

    @Override
    public final short getLength() {
        return length;
    }

    /**
     * 返回适合日志记录的 Channel 接入信号描述。
     *
     * <p>具体子类名称用于区分请求和响应；一次性接入令牌等专属载荷不会写入字符串，
     * 避免凭证通过日志泄露。</p>
     */
    @Override
    public String toString() {
        return super.toString() + ", Kind: " + kind;
    }

    /** 将 Kind 之后的专属载荷写入 LoopBuffer。 */
    protected abstract void writePayload(LoopBuffer buffer);

    private static void validateLength(int contentLength, int minimumLength, int maximumLength) {
        if (contentLength < minimumLength || contentLength > maximumLength) {
            throw new IllegalArgumentException(
                    "Invalid channel access signal length: " + contentLength
                            + ", expected range: [" + minimumLength + ", " + maximumLength + "]"
            );
        }
    }
}
