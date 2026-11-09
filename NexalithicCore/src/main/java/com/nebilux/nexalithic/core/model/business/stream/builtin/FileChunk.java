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
 * 文件流中一个不可变的完整数据块。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class FileChunk extends BusinessStream.Chunk {
    private final long offset;
    private final byte[] data;

    public FileChunk(long sequence, long offset, byte[] data) {
        super(sequence);
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        this.data = Objects.requireNonNull(data, "data").clone();
        if (this.data.length == 0) {
            throw new IllegalArgumentException("file chunk must not be empty");
        }
        this.offset = offset;
    }

    public long offset() {
        return offset;
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
    public static final class Codec implements ChunkCodec<FileChunk> {
        public static final int UID = 2;

        @Override
        public int id() {
            return UID;
        }

        @Override
        public Class<FileChunk> clazz() {
            return FileChunk.class;
        }

        @Override
        public Encoder<FileChunk> encoder() {
            return null;
        }

        @Override
        public Decoder<FileChunk> decoder() {
            return null;
        }
    }
}
