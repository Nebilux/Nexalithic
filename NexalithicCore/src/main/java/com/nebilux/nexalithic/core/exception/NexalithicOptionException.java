package com.nebilux.nexalithic.core.exception;

/**
 * Nexalithic 配置异常
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class NexalithicOptionException extends NexalithicException {
    public NexalithicOptionException(String optionName, String detail) {
        super("Option error at [" + optionName + "]: " + detail, true);
    }
}