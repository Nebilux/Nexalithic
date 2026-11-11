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
        this(sequence, type, timestampMicros, keyFrame, data, true);
    }

    public MediaChunk(long sequence, Type type, long timestampMicros, boolean keyFrame, byte[] data, boolean copyData) {
        super(sequence);
        this.type = Objects.requireNonNull(type, "type");
        if (timestampMicros < 0) {
            throw new IllegalArgumentException("timestampMicros must be non-negative");
        }
        if (keyFrame && type != Type.Video) {
            throw new IllegalArgumentException("only video chunks can be key frames");
        }
        byte[] checkedData = Objects.requireNonNull(data, "data");
        if (checkedData.length == 0) {
            throw new IllegalArgumentException("media chunk must not be empty");
        }
        this.data = copyData ? checkedData.clone() : checkedData;
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
    public static final class Codec implements ChunkCodec<MediaChunk> {
        public static final int UID = 1;
        /** 类型 1 字节、微秒时间戳 8 字节、关键帧标记 1 字节；后接媒体数据。 */
        private static final int METADATA_SIZE = Byte.BYTES + Long.BYTES + Byte.BYTES;

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
            return new MediaChunkEncoder();
        }

        @Override
        public Decoder<MediaChunk> decoder() {
            return new MediaChunkDecoder();
        }

        private static final class MediaChunkEncoder implements Encoder<MediaChunk> {
            private byte typeCode;
            private long timestampMicros;
            private byte keyFrameCode;
            private byte[] data;
            private boolean metadataWritten;
            private int dataOffset;

            @Override
            public int prepare(MediaChunk chunk) throws IOException {
                reset();
                if (chunk.data.length > Integer.MAX_VALUE - METADATA_SIZE) {
                    throw new IOException("Encoded media chunk length exceeds int range");
                }
                typeCode = switch (chunk.type) {
                    case Audio -> 0;
                    case Video -> 1;
                };
                timestampMicros = chunk.timestampMicros;
                keyFrameCode = (byte) (chunk.keyFrame ? 1 : 0);
                data = chunk.data;
                return METADATA_SIZE + data.length;
            }

            @Override
            public void encode(LoopBuffer.LimitedWritableView target) {
                if (!metadataWritten) {
                    if (target.remaining() < METADATA_SIZE) {
                        return;
                    }
                    target.putByte(typeCode);
                    target.putLong(timestampMicros);
                    target.putByte(keyFrameCode);
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
                typeCode = 0;
                timestampMicros = 0;
                keyFrameCode = 0;
                data = null;
                metadataWritten = false;
                dataOffset = 0;
            }

            @Override
            public void clear() {
                reset();
            }
        }

        private static final class MediaChunkDecoder implements Decoder<MediaChunk> {
            private long sequence;
            private byte typeCode;
            private long timestampMicros;
            private byte keyFrameCode;
            private byte[] data;
            private boolean metadataRead;
            private int dataOffset;

            @Override
            public void prepare(long sequence, int size) throws IOException {
                reset();
                if (sequence < 0 || size <= METADATA_SIZE) {
                    throw new IOException("Invalid media chunk sequence or encoded length");
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
                    typeCode = source.getByte();
                    timestampMicros = source.getLong();
                    keyFrameCode = source.getByte();
                    metadataRead = true;
                }
                dataOffset += source.writeTo(data, dataOffset);
            }

            @Override
            public MediaChunk tryFinish() throws IOException {
                if (data == null || !metadataRead || dataOffset != data.length) {
                    return null;
                }
                Type type = switch (typeCode) {
                    case 0 -> Type.Audio;
                    case 1 -> Type.Video;
                    default -> throw new IOException("Unknown media chunk type: " + Byte.toUnsignedInt(typeCode));
                };
                if (timestampMicros < 0) {
                    throw new IOException("Negative media chunk timestamp");
                }
                if (keyFrameCode != 0 && keyFrameCode != 1) {
                    throw new IOException("Invalid media chunk key frame marker: " + Byte.toUnsignedInt(keyFrameCode));
                }
                boolean keyFrame = keyFrameCode == 1;
                if (keyFrame && type != Type.Video) {
                    throw new IOException("Audio chunk cannot be a key frame");
                }
                return new MediaChunk(sequence, type, timestampMicros, keyFrame, data, false);
            }

            @Override
            public void reset() {
                sequence = 0;
                typeCode = 0;
                timestampMicros = 0;
                keyFrameCode = 0;
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
