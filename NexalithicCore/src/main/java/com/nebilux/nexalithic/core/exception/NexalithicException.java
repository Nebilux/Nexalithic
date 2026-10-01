package com.nebilux.nexalithic.core.exception;

/**
 * Nexalithic异常基类
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class NexalithicException extends RuntimeException {
    public NexalithicException(String message) {
        super(message);
    }

    protected NexalithicException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected NexalithicException(String message, boolean writableStackTrace) {
        super(message, null, false, writableStackTrace);
    }
}