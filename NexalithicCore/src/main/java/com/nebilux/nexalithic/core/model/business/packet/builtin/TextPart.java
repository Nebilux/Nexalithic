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

package com.nebilux.nexalithic.core.model.business.packet.builtin;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 文本有效载荷
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class TextPart extends BusinessPacket.Part<String> {
    private static final byte TYPE_NULL = 0;
    private static final byte TYPE_EMPTY = 1;
    private static final byte TYPE_NORMAL = 2;

    public TextPart(String text) {
        super(text);
    }

    @Override
    public long logicalSize() {
        return value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public TextPart duplicate() {
        return new TextPart(value);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (value == null ? ": null" : ": \"" + value + "\"");
    }

    /**
     * 编解码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    public static final class Codec implements PartCodec<TextPart> {
        public static final int UID = 1;

        @Override
        public int id() {
            return UID;
        }

        @Override
        public Class<TextPart> clazz() {
            return TextPart.class;
        }

        @Override
        public Encoder<TextPart> encoder() {
            return new TextPartEncoder();
        }

        @Override
        public Decoder<TextPart> decoder() {
            return new TextPartDecoder();
        }

        private static final class TextPartEncoder implements Encoder<TextPart> {
            private byte type;
            private byte[] bytes;
            private int processed;

            @Override
            public long prepare(TextPart part) {
                String value = part.value();
                type = value == null ? TYPE_NULL : value.isEmpty() ? TYPE_EMPTY : TYPE_NORMAL;
                bytes = type == TYPE_NORMAL ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                return 1L + bytes.length;
            }

            @Override
            public void encode(LoopBuffer.LimitedWritableView target) {
                if (target.remaining() == 0) {
                    return;
                }
                if (processed == 0) {
                    target.putByte(type);
                    processed = 1;
                }
                processed += target.readFrom(bytes, processed - 1);
            }

            @Override
            public boolean tryFinish() {
                return bytes != null && processed == bytes.length + 1;
            }

            @Override
            public void clear() {
                bytes = null;
                processed = 0;
            }
        }

        private static final class TextPartDecoder implements Decoder<TextPart> {
            private long size;
            private int processed;
            private byte type = -1;
            private byte[] bytes;

            @Override
            public void prepare(long size) throws IOException {
                clear();
                if (size < 1 || size > Integer.MAX_VALUE) {
                    throw new IOException("Invalid text payload length: " + size);
                }
                this.size = size;
            }

            @Override
            public void decode(LoopBuffer.LimitedReadableView source) throws IOException {
                if (source.remaining() == 0) {
                    return;
                }
                if (processed == 0) {
                    type = source.getByte();
                    processed = 1;
                    if (type == TYPE_NORMAL) {
                        bytes = new byte[(int) size - 1];
                    } else if ((type != TYPE_NULL && type != TYPE_EMPTY) || size != 1) {
                        throw new IOException("Invalid text payload type or length");
                    }
                }
                if (bytes != null) {
                    int n = source.writeTo(bytes, processed - 1);
                    processed += n;
                }
            }

            @Override
            public TextPart tryFinish() {
                if (processed != size) {
                    return null;
                }
                return new TextPart(type == TYPE_NULL ? null
                        : type == TYPE_EMPTY ? "" : new String(bytes, StandardCharsets.UTF_8));
            }

            @Override
            public void clear() {
                size = 0;
                processed = 0;
                type = -1;
                bytes = null;
            }
        }
    }
}
