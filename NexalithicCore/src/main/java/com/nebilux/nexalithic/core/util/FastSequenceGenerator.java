package com.nebilux.nexalithic.core.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 快速序列生成器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/10
 * @version 1.0.0
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