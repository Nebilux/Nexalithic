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

package com.nebilux.nexalithic.core.internal.model.stream;

import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.StreamFeeder;

import java.util.Objects;

/**
 * 流编解码器访问块队列的内部接口。
 *
 * @param <C> 流块类型
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface StreamCodecAccess<C extends BusinessStream.Chunk> {
    /** 从公开流模型取得编解码入口；不支持此入口的实现会被拒绝。 */
    @SuppressWarnings("unchecked")
    static <C extends BusinessStream.Chunk> StreamCodecAccess<C> from(BusinessStream<C> stream) {
        Objects.requireNonNull(stream, "stream");
        if (!(stream instanceof StreamCodecAccess<?> access)) {
            throw new IllegalArgumentException("Unsupported BusinessStream implementation");
        }
        return (StreamCodecAccess<C>) access;
    }

    /** 取得下一个待发送的块；暂无块时返回 {@code null}。 */
    C pollInputChunk();

    /** 输入已结束且待发送块均已取走。 */
    boolean isInputFinished();

    /** 输入由空变为可读或结束时通知编解码器。 */
    void onInputAvailable(Runnable callback);

    /** 向输出队列交付完整块。 */
    StreamFeeder.OfferResult offerOutputChunk(C chunk);

    /** 收到结束标记后结束输出。 */
    void finishOutput();

    /** 输出队列恢复容量时通知编解码器。 */
    void onOutputAvailable(Runnable callback);
}
