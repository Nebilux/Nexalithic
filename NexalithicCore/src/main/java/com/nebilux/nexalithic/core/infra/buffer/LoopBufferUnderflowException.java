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

import com.nebilux.nexalithic.core.exception.NexalithicBufferException;

/**
 * 循环缓冲区下溢异常（物理数据不足）
 * <p>当尝试从 {@code LoopBuffer} 读取数据，但缓冲区处于 {@code isEmpty()} 状态时抛出。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class LoopBufferUnderflowException extends NexalithicBufferException {

    /**
     * @param requested 请求读取的字节数
     * @param readable 实际可读的字节数
     */
    public LoopBufferUnderflowException(int requested, int readable) {
        super(String.format("Physical buffer underflow: requested %d bytes, but only %d bytes readable", 
              requested, readable));
    }
}