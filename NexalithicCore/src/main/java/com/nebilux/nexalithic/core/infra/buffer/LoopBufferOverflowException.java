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

package com.nebilux.nexalithic.core.infra.buffer;

/**
 * 循环缓冲区物理可写空间不足的异常。
 *
 * <p>一次写入所需字节数大于缓冲区当前可写字节数时产生；
 * 与有限视图的逻辑配额不足不同。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class LoopBufferOverflowException extends LoopBufferException {

    private LoopBufferOverflowException(String message) {
        super(message);
    }

    /**
     * @param required 请求写入的字节数
     * @param available 缓冲区当前实际可写的物理空间
     */
    public static LoopBufferOverflowException forWrite(int required, int available) {
        return new LoopBufferOverflowException(
                String.format("Physical buffer overflow: requested %d bytes, but only %d available", required, available)
        );
    }
}
