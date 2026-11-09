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

package com.nebilux.nexalithic.core.model.business.stream.codec;

import com.nebilux.nexalithic.core.model.business.codec.PayloadCodec;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;

import java.io.IOException;

/**
 * 块编解码器
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface ChunkCodec<C extends BusinessStream.Chunk> extends PayloadCodec<C> {
    Encoder<C> encoder();

    Decoder<C> decoder();

    /**
     * 编码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    interface Encoder<C extends BusinessStream.Chunk> extends PayloadCodec.Encoder {
        int prepare(C chunk) throws IOException;

        /** 完成一个 Chunk 后重置状态，以便编码下一个 Chunk。 */
        void reset();
    }

    /**
     * 译码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    interface Decoder<C extends BusinessStream.Chunk> extends PayloadCodec.Decoder<C> {
        void prepare(long sequence, int size) throws IOException;

        /** 完成一个 Chunk 后重置状态，以便解码下一个 Chunk。 */
        void reset();
    }
}
