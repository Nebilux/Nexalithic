package com.nebilux.nexalithic.core.infra.buffer;

import com.nebilux.nexalithic.core.exception.NexalithicBufferException;

/**
 * 循环缓冲区溢出异常（物理空间耗尽）
 * <p>当尝试向 {@code LoopBuffer} 写入数据，但其剩余空间不足以容纳请求的字节数时抛出。</p>
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/13
 * @version 1.0.1
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
