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

package com.nebilux.nexalithic.core.model.packet.signaling;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;

import java.util.Arrays;

/**
 * 原始信号
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class RawSignal extends SignalingPacket {
    private byte[] content;

    public RawSignal(byte signal, byte[] content) {
        super(signal);
        if (content == null) {
            return;
        }
        if (content.length + HEADER_LENGTH > MAX_PACKET_LENGTH) {
            throw new IllegalArgumentException("Packet length exceeds maximum of " + MAX_PACKET_LENGTH);
        }
        this.length = (short) content.length;
        this.content = content;
    }

    public void onToBuffer(LoopBuffer buffer) {
        if (content != null) {
            buffer.unsafePut(content, length);
        }
    }

    public short getLength() {
        return length;
    }
    public byte[] getContent() {
        return content;
    }

    @Override
    public String toString() {
        return super.toString() + ", Length: " + length + ", Content" + Arrays.toString(content);
    }
}
