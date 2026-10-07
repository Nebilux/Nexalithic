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
 * 循环缓冲区溢出异常（物理空间耗尽）
 * <p>当尝试向 {@code LoopBuffer} 写入数据，但其剩余空间不足以容纳请求的字节数时抛出。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class LoopBufferOverflowException extends NexalithicBufferException {

    /**
     * @param required 请求写入的字节数
     * @param available 缓冲区当前实际可写的物理空间
     */
    public LoopBufferOverflowException(int required, int available) {
        super(String.format("Physical buffer overflow: requested %d bytes, but only %d available", required, available));
    }
}
