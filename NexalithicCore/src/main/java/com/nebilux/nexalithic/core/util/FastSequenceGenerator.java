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

package com.nebilux.nexalithic.core.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 快速序列生成器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class FastSequenceGenerator {
    private final AtomicLong state = new AtomicLong(0L);

    public long nextId() {
        while (true) {
            long current = state.get();
            long now = System.currentTimeMillis();
            long last = current >>> 16;
            long next;
            if (now > last) {
                next = (now << 16);
            } else {
                next = current + 1;
            }
            if (state.compareAndSet(current, next)) {
                return next;
            }
        }
    }
}