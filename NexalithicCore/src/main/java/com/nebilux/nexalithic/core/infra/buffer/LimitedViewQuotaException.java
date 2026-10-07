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
 * 有限视图配额异常（逻辑边界越界）
 * <p>当业务层通过 {@code LimitedView} 尝试读写超过预设 Quota 的数据时抛出。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class LimitedViewQuotaException extends NexalithicBufferException {

    /**
     * @param quota 当前视图剩余的配额
     * @param required 业务请求操作的字节数
     */
    public LimitedViewQuotaException(int quota, int required) {
        super(String.format("Logical quota exceeded: remaining %d, requested %d", quota, required));
    }
}
