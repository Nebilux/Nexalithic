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

import java.util.Objects;

/**
 * Nexalithic重复Key异常。
 *
 * <p>当注册表、映射表或配置集合中出现不允许重复的Key时抛出。</p>
 *
 * @author Reonvia
 * @since 0.2.0
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