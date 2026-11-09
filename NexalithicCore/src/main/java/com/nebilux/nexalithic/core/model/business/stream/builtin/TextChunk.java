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

package com.nebilux.nexalithic.core.model.business.stream.builtin;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodec;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 文本块
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class TextChunk extends BusinessStream.Chunk {
    private final String content;
    private final byte[] bytes;
    private final int size;

    public TextChunk(String content, long sequence) {
        super(sequence);
        this.content = Objects.requireNonNull(content, "content");
        this.bytes = content.getBytes(StandardCharsets.UTF_8);
        this.size = bytes.length;
    }

    private TextChunk(byte[] bytes, long sequence) {
        super(sequence);
        this.bytes = bytes;
        this.content = new String(bytes, StandardCharsets.UTF_8);
        this.size = bytes.length;
    }

    public String content() {
        return content;
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * 编解码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    public static final class Codec implements ChunkCodec<TextChunk> {
        public static final int UID = 3;

        @Override
        public int id() {
            return UID;
        }

        @Override
        public Class<TextChunk> clazz() {
            return TextChunk.class;
        }

        @Override
        public Encoder<TextChunk> encoder() {
            return new TextChunkEncoder();
        }

        @Override
        public Decoder<TextChunk> decoder() {
            return new TextChunkDecoder();
        }

        private static final class TextChunkEncoder implements Encoder<TextChunk> {
            private byte[] bytes;
            private int processed;

            @Override
            public int prepare(TextChunk chunk) {
                reset();
                bytes = chunk.bytes;
                return chunk.size;
            }

            @Override
            public void encode(LoopBuffer.LimitedWritableView target) {
                int encoded = target.readFrom(bytes, processed);
                processed += encoded;
            }

            @Override
            public boolean tryFinish() {
                return processed == bytes.length;
            }

            @Override
            public void reset() {
                bytes = null;
                processed = 0;
            }

            @Override
            public void clear() {
                reset();
            }
        }

        private static final class TextChunkDecoder implements Decoder<TextChunk> {
            private long sequence;
            private byte[] bytes;
            private int processed;

            @Override
            public void prepare(long sequence, int size) {
                reset();
                this.sequence = sequence;
                this.bytes = new byte[size];
            }

            @Override
            public void decode(LoopBuffer.LimitedReadableView source) {
                int decoded = source.writeTo(bytes, processed);
                processed += decoded;
            }

            @Override
            public TextChunk tryFinish() {
                if (processed == bytes.length) {
                    return new TextChunk(bytes, sequence);
                } else {
                    return null;
                }
            }

            @Override
            public void reset() {
                sequence = 0;
                bytes = null;
                processed = 0;
            }

            @Override
            public void clear() {
                reset();
            }
        }
    }
}
