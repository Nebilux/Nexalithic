package com.nebilux.nexalithic.core.infra.buffer;

import com.nebilux.nexalithic.core.exception.NexalithicBufferException;

/**
 * 有限视图配额异常（逻辑边界越界）
 * <p>当业务层通过 {@code LimitedView} 尝试读写超过预设 Quota 的数据时抛出。</p>
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/13
 * @version 1.0.1
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
