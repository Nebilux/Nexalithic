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

import java.io.IOException;
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
        this(sequence, offset, data, true);
    }

    private FileChunk(long sequence, long offset, byte[] data, boolean copyData) {
        super(sequence);
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        byte[] checkedData = Objects.requireNonNull(data, "data");
        if (checkedData.length == 0) {
            throw new IllegalArgumentException("file chunk must not be empty");
        }
        this.data = copyData ? checkedData.clone() : checkedData;
        this.offset = offset;
    }

    public long offset() {
        return offset;
    }

    public byte[] data(boolean copyData) {
        return copyData ? data.clone() : data;
    }

    public byte[] data() {
        return data(false);
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
        /** 块序号与编码长度由流帧承载；块内仅编码文件偏移和数据。 */
        private static final int METADATA_SIZE = Long.BYTES;

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
            return new FileChunkEncoder();
        }

        @Override
        public Decoder<FileChunk> decoder() {
            return new FileChunkDecoder();
        }

        private static final class FileChunkEncoder implements Encoder<FileChunk> {
            private long offset;
            private byte[] data;
            private boolean metadataWritten;
            private int dataOffset;

            @Override
            public int prepare(FileChunk chunk) throws IOException {
                reset();
                if (chunk.data.length > Integer.MAX_VALUE - METADATA_SIZE) {
                    throw new IOException("Encoded file chunk length exceeds int range");
                }
                offset = chunk.offset;
                data = chunk.data;
                return METADATA_SIZE + data.length;
            }

            @Override
            public void encode(LoopBuffer.LimitedWritableView target) {
                if (!metadataWritten) {
                    if (target.remaining() < METADATA_SIZE) {
                        return;
                    }
                    target.putLong(offset);
                    metadataWritten = true;
                }
                dataOffset += target.readFrom(data, dataOffset);
            }

            @Override
            public boolean tryFinish() {
                return data != null && metadataWritten && dataOffset == data.length;
            }

            @Override
            public void reset() {
                offset = 0;
                data = null;
                metadataWritten = false;
                dataOffset = 0;
            }

            @Override
            public void clear() {
                reset();
            }
        }

        private static final class FileChunkDecoder implements Decoder<FileChunk> {
            private long sequence;
            private long offset;
            private byte[] data;
            private boolean metadataRead;
            private int dataOffset;

            @Override
            public void prepare(long sequence, int size) throws IOException {
                reset();
                if (sequence < 0 || size <= METADATA_SIZE) {
                    throw new IOException("Invalid file chunk sequence or encoded length");
                }
                this.sequence = sequence;
                data = new byte[size - METADATA_SIZE];
            }

            @Override
            public void decode(LoopBuffer.LimitedReadableView source) {
                if (!metadataRead) {
                    if (source.remaining() < METADATA_SIZE) {
                        return;
                    }
                    offset = source.getLong();
                    metadataRead = true;
                }
                dataOffset += source.writeTo(data, dataOffset);
            }

            @Override
            public FileChunk tryFinish() throws IOException {
                if (data == null || !metadataRead || dataOffset != data.length) {
                    return null;
                }
                if (offset < 0) {
                    throw new IOException("Negative file chunk offset");
                }
                return new FileChunk(sequence, offset, data, false);
            }

            @Override
            public void reset() {
                sequence = 0;
                offset = 0;
                data = null;
                metadataRead = false;
                dataOffset = 0;
            }

            @Override
            public void clear() {
                reset();
            }
        }
    }
}
