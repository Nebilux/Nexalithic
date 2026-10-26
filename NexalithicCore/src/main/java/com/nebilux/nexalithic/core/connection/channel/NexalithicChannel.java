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
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.signaling.packet.SignalingPacket;

import java.io.IOException;

/**
 * Nexalithic 通道
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface NexalithicChannel {
    enum Kind {
        Signaling_Packet((byte) 0x01, NexalithicModel.Form.Packet, SignalingPacket.class),
        Business_Packet((byte) 0x11, NexalithicModel.Form.Packet, BusinessPacket.class),
        Business_Stream((byte) 0x12, NexalithicModel.Form.Stream, BusinessStream.class),
        ;

        private final byte code;
        private final NexalithicModel.Form form;
        private final Class<? extends NexalithicModel> clazz;

        Kind(byte code, NexalithicModel.Form form, Class<? extends NexalithicModel> clazz) {
            this.code = code;
            this.form = form;
            this.clazz = clazz;
        }

        public static Kind from(byte code) {
            return switch (code) {
                case 0x01 -> Signaling_Packet;
                case 0x11 -> Business_Packet;
                case 0x12 -> Business_Stream;
                default -> throw new IllegalArgumentException(
                        "Unknown channel kind protocol id: " + Byte.toUnsignedInt(code)
                );
            };
        }

        public byte code() {
            return code;
        }

        public NexalithicModel.Form form() {
            return form;
        }
        public Class<? extends NexalithicModel> clazz() {
            return clazz;
        }

        public boolean isSignaling() {
            return this == Signaling_Packet;
        }
        public boolean isBusiness() {
            return !isSignaling();
        }
        public boolean isPacket() {
            return form() == NexalithicModel.Form.Packet;
        }
        public boolean isStream() {
            return form() == NexalithicModel.Form.Stream;
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
    default boolean isClose() {
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
