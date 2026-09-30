package com.thezeroer.nexalithic.core.io.channel;

import com.thezeroer.nexalithic.core.model.AbstractModel;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.stream.AbstractStream;

import java.io.IOException;

/**
 * Nexalithic 通道
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/07
 * @version 1.0.0
 */
public interface NexalithicChannel {
    enum Kind {
        Packet_Signaling(AbstractModel.ModelType.Packet),
        Packet_Business(AbstractModel.ModelType.Packet),
        Stream_Media(AbstractModel.ModelType.Stream),
        Stream_File(AbstractModel.ModelType.Stream);

        private final AbstractModel.ModelType model;
        Kind(AbstractModel.ModelType model) {
            this.model = model;
        }

        public AbstractModel.ModelType modelType() {
            return model;
        }
        public AbstractPacket.PacketType packetType() {
            return switch (this) {
                case Packet_Signaling -> AbstractPacket.PacketType.Signaling;
                case Packet_Business -> AbstractPacket.PacketType.Business;
                default -> throw new IllegalStateException(
                        "Channel kind %s does not carry packetType".formatted(this)
                );
            };
        }
        public AbstractStream.StreamType streamType() {
            return switch (this) {
                case Stream_Media -> AbstractStream.StreamType.Media;
                case Stream_File -> AbstractStream.StreamType.File;
                default -> throw new IllegalStateException(
                        "Channel kind %s does not carry streamType".formatted(this)
                );
            };
        }
        public boolean isPacket() {
            return modelType() == AbstractModel.ModelType.Packet;
        }
        public boolean isStream() {
            return modelType() == AbstractModel.ModelType.Stream;
        }
    }
    enum State {
        Opening,
        Opened,
        Closing,
        Closed;

        public boolean isOpened() {
            return this == Opened;
        }
        public boolean isClosed() {
            return this == Closed;
        }
        public boolean isTransitioning() {
            return this == Opening || this == Closing;
        }
    }

    State getState();
    default boolean isOpen() {
        State state = getState();
        return state == State.Opening || state == State.Opened;
    }
    default boolean isClos() {
        State state = getState();
        return state == State.Closing || state == State.Closed;
    }

    void updateLastActiveTimeNanos(long lastActiveTimeNanos);
    /**
     * 返回最近一次有效 I/O 的时间点，基于 System.nanoTime()。
     *
     * @return 尚未发生有效 I/O 时返回 -1
     */
    long getLastActiveTimeNanos();
    /**
     * 关闭当前物理连接。
     *
     * <p>只释放当前物理连接，不销毁 Channel 对象；
     * 关闭完成后 Channel 可以再次打开。</p>
     *
     * @return 当前调用是否成功发起关闭；已处于 CLOSED 或 CLOSING 时返回 false
     */
    boolean close() throws IOException;
}
