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
 * 有限视图逻辑配额不足的异常。
 *
 * <p>通过缓冲区有限视图读写时，请求的字节数超过该视图剩余配额就会产生，
 * 即使底层缓冲区仍有足够的物理空间或数据。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class LimitedViewQuotaException extends LoopBufferException {

    private LimitedViewQuotaException(String message) {
        super(message);
    }

    /**
     * @param quota 当前视图剩余的配额
     * @param required 业务请求操作的字节数
     */
    public static LimitedViewQuotaException exceeded(int quota, int required) {
        return new LimitedViewQuotaException(
                String.format("Logical quota exceeded: remaining %d, requested %d", quota, required)
        );
    }
}
