package com.nebilux.nexalithic.core.builder.module;

import java.util.Objects;

/**
 * Nexalithic 模块
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/04
 * @version 1.0.0
 */
public final class NexalithicModule<T> {
    private final String name;
    private final Class<T> type;

    NexalithicModule(String name, Class<T> type) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
    }

    public String name() {
        return name;
    }
    public Class<T> type() {
        return type;
    }
}
