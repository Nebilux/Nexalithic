package com.nebilux.nexalithic.core.exception;

/**
 * Nexalithic 缓冲区异常
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/13
 * @version 1.0.0
 */
public class NexalithicBufferException extends NexalithicException {
    protected NexalithicBufferException(String message) {
        super(message, true);
    }
}