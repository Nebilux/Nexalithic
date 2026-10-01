package com.nebilux.nexalithic.core.exception;

/**
 * Nexalithic 缓冲区异常
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class NexalithicBufferException extends NexalithicException {
    protected NexalithicBufferException(String message) {
        super(message, true);
    }
}