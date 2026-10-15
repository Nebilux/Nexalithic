/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.connection.channel;

import com.nebilux.nexalithic.core.model.NexalithicModel;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.model.stream.AbstractStream;
import com.nebilux.nexalithic.core.model.stream.file.FileStream;
import com.nebilux.nexalithic.core.model.stream.media.MediaStream;

import java.io.IOException;

/**
 * Nexalithic 通道
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface NexalithicChannel {
    enum Kind {
        Packet_Signaling((byte) 0x0, NexalithicModel.Model.Packet, SignalingPacket.class),
        Packet_Business((byte) 0x1, NexalithicModel.Model.Packet, BusinessPacket.class),
        Stream_Media((byte) 0x2, NexalithicModel.Model.Stream, MediaStream.class),
        Stream_File((byte) 0x3, NexalithicModel.Model.Stream, FileStream.class);

        private final byte code;
        private final NexalithicModel.Model model;
        private final Class<? extends NexalithicModel> clazz;

        Kind(byte code, NexalithicModel.Model model, Class<? extends NexalithicModel> clazz) {
            this.code = code;
            this.model = model;
            this.clazz = clazz;
        }

        public static Kind fromCode(byte code) {
            return switch (code) {
                case 0x0 -> Packet_Signaling;
                case 0x1 -> Packet_Business;
                case 0x2 -> Stream_Media;
                case 0x3 -> Stream_File;
                default -> throw new IllegalArgumentException(
                        "Unknown channel kind protocol id: " + Byte.toUnsignedInt(code)
                );
            };
        }

        public byte code() {
            return code;
        }

        public NexalithicModel.Model model() {
            return model;
        }
        public Class<? extends NexalithicModel> clazz() {
            return clazz;
        }
        public AbstractPacket.Type packetType() {
            return switch (this) {
                case Packet_Signaling -> AbstractPacket.Type.Signaling;
                case Packet_Business -> AbstractPacket.Type.Business;
                default -> throw new IllegalStateException(
                        "Channel kind %s does not carry packetType".formatted(this)
                );
            };
        }
        public AbstractStream.Type streamType() {
            return switch (this) {
                case Stream_Media -> AbstractStream.Type.Media;
                case Stream_File -> AbstractStream.Type.File;
                default -> throw new IllegalStateException(
                        "Channel kind %s does not carry streamType".formatted(this)
                );
            };
        }
        public boolean isPacket() {
            return model() == NexalithicModel.Model.Packet;
        }
        public boolean isStream() {
            return model() == NexalithicModel.Model.Stream;
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

    Kind getKind();

    State getState();
    default boolean isOpen() {
        State state = getState();
        return state == State.Opening || state == State.Opened;
    }
    default boolean isClos() {
        State state = getState();
        return state == State.Closing || state == State.Closed;
    }

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
