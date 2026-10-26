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

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;

import java.io.IOException;

/**
 * 块编解码器
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface ChunkCodec<C extends BusinessStream.Chunk> {
    int id();
    Class<C> clazz();

    Encoder<C> encoder();
    Decoder<C> decoder();

    /**
     * 编码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    interface Encoder<C extends BusinessStream.Chunk> {
        int prepare(C chunk) throws IOException;
        int encode(LoopBuffer.LimitedWritableView target) throws IOException;
        boolean tryFinish() throws IOException;
        void reset();
    }

    /**
     * 译码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    interface Decoder<C extends BusinessStream.Chunk> {
        void prepare(long sequence, int size) throws IOException;
        int decode(LoopBuffer.LimitedReadableView source) throws IOException;
        C tryFinish() throws IOException;
        void reset();
    }
}
