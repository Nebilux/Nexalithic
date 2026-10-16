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

package com.nebilux.nexalithic.core.model.business.stream.media;

import com.nebilux.nexalithic.core.model.business.stream.AbstractChunk;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * 一帧或一个音频块。载荷的编码格式由调用方约定，序号缺口允许表示丢帧。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public final class MediaChunk extends AbstractChunk {
    public enum Kind { Audio, Video }

    private final Kind kind;
    private final long timestampMicros;
    private final boolean keyFrame;
    private final byte[] data;

    public MediaChunk(long sequence, Kind kind, long timestampMicros, boolean keyFrame, byte[] data) {
        super(sequence);
        this.kind = Objects.requireNonNull(kind, "kind");
        if (timestampMicros < 0) {
            throw new IllegalArgumentException("timestampMicros must be non-negative");
        }
        if (keyFrame && kind != Kind.Video) {
            throw new IllegalArgumentException("only video chunks can be key frames");
        }
        this.data = Objects.requireNonNull(data, "data").clone();
        if (this.data.length == 0) {
            throw new IllegalArgumentException("media chunk must not be empty");
        }
        this.timestampMicros = timestampMicros;
        this.keyFrame = keyFrame;
    }

    public Kind kind() {
        return kind;
    }

    public long timestampMicros() {
        return timestampMicros;
    }

    public boolean keyFrame() {
        return keyFrame;
    }

    public int size() {
        return data.length;
    }

    public ByteBuffer data() {
        return ByteBuffer.wrap(data).asReadOnlyBuffer();
    }
}
