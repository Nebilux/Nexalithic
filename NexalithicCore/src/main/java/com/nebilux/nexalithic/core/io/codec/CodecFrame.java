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

package com.nebilux.nexalithic.core.io.codec;

/**
 * 包与流共用的编解码帧头。
 *
 * <p>帧头占 8 字节，以大端序写入：高 2 位为类型，随后 14 位保留，
 * 接着 16 位为载荷长度，低 32 位为通道内的模型序号。类型的具体语义由所属通道决定。</p>
 *
 * @author Reonvia
 * @since 0.5.0
 */
public final class CodecFrame {
    public static final int HEADER_LENGTH = Long.BYTES;
    public static final int MAX_PAYLOAD_LENGTH = 1024 * 16;

    private static final int TYPE_SHIFT = 62;
    private static final int PAYLOAD_LENGTH_SHIFT = 32;
    private static final long PAYLOAD_LENGTH_MASK = 0xFFFFL;
    private static final long MODEL_SEQUENCE_MASK = 0xFFFF_FFFFL;

    /** 模型帧类型。所有类型均可携带载荷，载荷格式由具体模型编解码器定义。 */
    public enum Type {
        /** 单帧承载完整模型，同时表示开始和结束。 */
        SINGLE(0),
        /** 多帧模型的首帧。 */
        START(1),
        /** 多帧模型的后续数据帧。 */
        DATA(2),
        /** 多帧模型的结束帧。 */
        END(3);

        private final int code;

        Type(int code) {
            this.code = code;
        }

        public boolean isSingle() {
            return this == SINGLE;
        }

        public boolean isStart() {
            return this == START;
        }

        public boolean isData() {
            return this == DATA;
        }

        public boolean isEnd() {
            return this == END;
        }

        private static Type fromCode(int code) {
            return switch (code) {
                case 0 -> SINGLE;
                case 1 -> START;
                case 2 -> DATA;
                case 3 -> END;
                default -> throw new IllegalArgumentException("Unknown codec frame type: " + code);
            };
        }
    }

    private CodecFrame() {}

    /** 将帧头字段打包为 64 位值。 */
    public static long pack(Type type, int modelSequence, int payloadLength) {
        return (long) type.code << TYPE_SHIFT | (long) payloadLength << PAYLOAD_LENGTH_SHIFT | (modelSequence & MODEL_SEQUENCE_MASK);
    }

    public static long packSingle(int modelSequence, int payloadLength) {
        return pack(Type.SINGLE, modelSequence, payloadLength);
    }

    public static long packStart(int modelSequence, int payloadLength) {
        return pack(Type.START, modelSequence, payloadLength);
    }

    public static long packData(int modelSequence, int payloadLength) {
        return pack(Type.DATA, modelSequence, payloadLength);
    }

    public static long packEnd(int modelSequence, int payloadLength) {
        return pack(Type.END, modelSequence, payloadLength);
    }

    /** 解析类型，并校验保留位、载荷长度及结束帧。 */
    public static Type parseFrameType(long header) {
        return Type.fromCode((int) (header >>> TYPE_SHIFT));
    }

    /** 解析载荷长度。 */
    public static int parsePayloadLength(long header) {
        return (int) ((header >>> PAYLOAD_LENGTH_SHIFT) & PAYLOAD_LENGTH_MASK);
    }

    /** 解析通道内的模型序号。 */
    public static int parseModelSequence(long header) {
        return (int) header;
    }
}
