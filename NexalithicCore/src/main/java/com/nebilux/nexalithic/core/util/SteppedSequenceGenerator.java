package com.nebilux.nexalithic.core.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 阶梯序列生成器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SteppedSequenceGenerator {
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
                if ((int) (current & 0xFFFF) < 0xFFFF) {
                    next = current + 1;
                } else {
                    Thread.onSpinWait();
                    continue;
                }
            }
            if (state.compareAndSet(current, next)) {
                return next;
            }
        }
    }
}