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

package com.nebilux.nexalithic.core.model.business.codec;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.business.BusinessModel;

import java.io.IOException;

/**
 * 有效载荷编解码器
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface PayloadCodec<T extends BusinessModel.Payload> {
    int id();

    Class<T> clazz();

    /**
     * 编码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    interface Encoder {
        /** 将数据写入受限视图；调用方根据视图剩余量的变化计算本次写入量。 */
        void encode(LoopBuffer.LimitedWritableView target) throws IOException;

        boolean tryFinish() throws IOException;

        /** 清理当前编码状态及其持有的资源；允许重复调用。 */
        void clear();
    }

    /**
     * 译码器
     *
     * @author Reonvia
     * @since 0.5.0
     */
    interface Decoder<T extends BusinessModel.Payload> {
        /** 从受限视图读取数据；调用方根据视图剩余量的变化计算本次读取量。 */
        void decode(LoopBuffer.LimitedReadableView source) throws IOException;

        T tryFinish() throws IOException;

        /** 清理当前解码状态及其持有的资源；允许重复调用。 */
        void clear();
    }
}
