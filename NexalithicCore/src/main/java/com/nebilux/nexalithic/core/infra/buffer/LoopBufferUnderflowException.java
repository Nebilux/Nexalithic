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
 * 循环缓冲区物理可读数据不足的异常。
 *
 * <p>一次读取所需字节数大于缓冲区当前可读字节数时产生；
 * 即使缓冲区并非完全为空，也可能发生。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class LoopBufferUnderflowException extends LoopBufferException {

    private LoopBufferUnderflowException(String message) {
        super(message);
    }

    /**
     * @param requested 请求读取的字节数
     * @param readable 实际可读的字节数
     */
    public static LoopBufferUnderflowException forRead(int requested, int readable) {
        return new LoopBufferUnderflowException(
                String.format("Physical buffer underflow: requested %d bytes, but only %d bytes readable", requested, readable)
        );
    }
}
