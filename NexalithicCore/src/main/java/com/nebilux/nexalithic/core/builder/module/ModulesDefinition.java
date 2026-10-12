/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.builder.module;

import java.util.Objects;

/**
 * 模定义
 *
 * @author Reonvia
 * @since 0.1.0
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
