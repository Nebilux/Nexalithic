package com.thezeroer.nexalithic.core.exception;

import java.util.Objects;

/**
 * Nexalithic重复Key异常。
 *
 * <p>当注册表、映射表或配置集合中出现不允许重复的Key时抛出。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/23
 */
public final class NexalithicDuplicateKeyException extends NexalithicConflictException {

    /**
     * 重复的Key。
     */
    private final Object key;

    public NexalithicDuplicateKeyException(Object key) {
        this(null, key);
    }

    public NexalithicDuplicateKeyException(String target, Object key) {
        super(createMessage(target, key));
        this.key = key;
    }

    public Object getKey() {
        return key;
    }

    private static String createMessage(String target, Object key) {
        String keyText = Objects.toString(key);
        if (target == null || target.isBlank()) {
            return "Duplicate key: " + keyText;
        }
        return "Duplicate key in " + target + ": " + keyText;
    }
}