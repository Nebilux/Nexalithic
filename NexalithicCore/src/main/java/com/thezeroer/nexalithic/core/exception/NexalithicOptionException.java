package com.thezeroer.nexalithic.core.exception;

/**
 * Nexalithic 配置异常
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/02
 * @version 1.0.0
 */
public final class NexalithicOptionException extends NexalithicException {
    public NexalithicOptionException(String optionName, String detail) {
        super("Option error at [" + optionName + "]: " + detail, true);
    }
}