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

import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodec;

import java.util.Objects;

/**
 * 一帧或一个音频块。载荷的编码格式由调用方约定，序号缺口允许表示丢帧。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class MediaChunk extends BusinessStream.Chunk {
    public enum Type { Audio, Video }

    private final Type type;
    private final long timestampMicros;
    private final boolean keyFrame;
    private final byte[] data;

    public MediaChunk(long sequence, Type type, long timestampMicros, boolean keyFrame, byte[] data) {
        super(sequence);
        this.type = Objects.requireNonNull(type, "type");
        if (timestampMicros < 0) {
            throw new IllegalArgumentException("timestampMicros must be non-negative");
        }
        if (keyFrame && type != Type.Video) {
            throw new IllegalArgumentException("only video chunks can be key frames");
        }
        this.data = Objects.requireNonNull(data, "data").clone();
        if (this.data.length == 0) {
            throw new IllegalArgumentException("media chunk must not be empty");
        }
        this.timestampMicros = timestampMicros;
        this.keyFrame = keyFrame;
    }

    public Type kind() {
        return type;
    }

    public long timestampMicros() {
        return timestampMicros;
    }

    public boolean keyFrame() {
        return keyFrame;
    }

    @Override
    public int size() {
        return data.length;
    }

    /**
     * 编解码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    public static final class Codec implements ChunkCodec<MediaChunk> {
        public static final int UID = 1;

        @Override
        public int id() {
            return UID;
        }

        @Override
        public Class<MediaChunk> clazz() {
            return MediaChunk.class;
        }

        @Override
        public Encoder<MediaChunk> encoder() {
            return null;
        }

        @Override
        public Decoder<MediaChunk> decoder() {
            return null;
        }
    }
}
