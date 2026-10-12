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

package com.nebilux.nexalithic.core.exception;

/**
 * 注册同一键时发现该键已经存在的异常。
 *
 * <p>通常发生在 Handler 映射、Controller 装配或拦截器注册时；
 * 可通过 {@link #getKey()} 获取发生冲突的键。</p>
 *
 * @author Reonvia
 * @since 0.2.0
 */
public final class NexalithicDuplicateKeyException extends NexalithicConflictException {

    /** 与现有注册项冲突的原始键。 */
    private final Object key;

    private NexalithicDuplicateKeyException(String message, Object key) {
        super(message);
        this.key = key;
    }

    public static NexalithicDuplicateKeyException forKey(Object key) {
        return new NexalithicDuplicateKeyException(createMessage(null, key), key);
    }

    public static NexalithicDuplicateKeyException inTarget(String target, Object key) {
        return new NexalithicDuplicateKeyException(createMessage(target, key), key);
    }

    public Object getKey() {
        return key;
    }

    private static String createMessage(String target, Object key) {
        String keyText = describeKey(key);
        if (target == null || target.isBlank()) {
            return "Duplicate key: " + keyText;
        }
        return "Duplicate key in " + target + ": " + keyText;
    }

    private static String describeKey(Object key) {
        if (key == null) {
            return "null";
        }
        if (key instanceof String text) {
            return text;
        }
        if (key instanceof Class<?> type) {
            return type.getName();
        }
        if (key instanceof Byte || key instanceof Short || key instanceof Integer || key instanceof Long
                || key instanceof Float || key instanceof Double || key instanceof Boolean || key instanceof Character) {
            return key.toString();
        }
        if (key instanceof Enum<?> constant) {
            return constant.getDeclaringClass().getName() + '.' + constant.name();
        }
        return key.getClass().getName() + '@' + Integer.toHexString(System.identityHashCode(key));
    }
}
