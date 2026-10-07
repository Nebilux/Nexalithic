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

package com.nebilux.nexalithic.core.model.packet.business;

import com.nebilux.nexalithic.core.exception.NexalithicException;

/**
 * 有效载荷溢出异常
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class PayloadOverflowException extends NexalithicException {
    private final int currentCount;    // 当前已有的数量
    private final int attemptedCount;  // 准备添加的数量
    private final int limit;           // 协议允许的最大数量

    public PayloadOverflowException(int currentCount, int attemptedCount, int limit) {
        super(String.format(
                "Payload overflow: Cannot add %d more payloads (Current: %d, Limit: %d).",
                attemptedCount, currentCount, limit
        ));
        this.currentCount = currentCount;
        this.attemptedCount = attemptedCount;
        this.limit = limit;
    }

    public int getCurrentCount() { return currentCount; }
    public int getAttemptedCount() { return attemptedCount; }
    public int getLimit() { return limit; }
}