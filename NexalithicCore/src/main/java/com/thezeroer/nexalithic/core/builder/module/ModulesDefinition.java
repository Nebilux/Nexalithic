package com.thezeroer.nexalithic.core.builder.module;

import java.util.Objects;

/**
 * 模定义
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/04
 * @version 1.0.0
 */
public abstract class ModulesDefinition {
    protected final Class<?> holder;

    protected ModulesDefinition(Class<?> holder) {
        this.holder = Objects.requireNonNull(holder, "holder");
    }

    @SuppressWarnings("unchecked")
    protected <T> NexalithicModule<T> defineModule(Class<?> type, String qualifier, String suffix) {
        return new NexalithicModule<>(formatName(holder, type, qualifier, suffix), (Class<T>) type);
    }
    protected <T> NexalithicModule<T> defineModule(Class<?> type, String qualifier) {
        return defineModule(type, qualifier, null);
    }
    protected <T> NexalithicModule<T> defineModule(Class<?> type) {
        return defineModule(type, null, null);
    }

    private String formatName(Class<?> holder, Class<?> type, String qualifier, String suffix) {
        return holder.getSimpleName() + "_" + (qualifier == null ? "" : qualifier) + type.getSimpleName() + (suffix == null ? "" : "_" + suffix);
    }
}
