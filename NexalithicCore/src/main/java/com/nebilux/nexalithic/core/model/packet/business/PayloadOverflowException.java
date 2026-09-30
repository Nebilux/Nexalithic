package com.nebilux.nexalithic.core.model.packet.business;

import com.nebilux.nexalithic.core.exception.NexalithicException;

/**
 * 有效载荷溢出异常
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/03
 * @version 1.0.0
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