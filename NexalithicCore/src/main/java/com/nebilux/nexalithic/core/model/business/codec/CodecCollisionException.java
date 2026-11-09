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

package com.nebilux.nexalithic.core.model.business.codec;

import com.nebilux.nexalithic.core.exception.NexalithicConflictException;

/**
 * Codec 标识在注册时发生冲突的异常。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class CodecCollisionException extends NexalithicConflictException {
    private final int conflictedId;
    private final Class<?> existingClass;
    private final Class<?> conflicting;

    private CodecCollisionException(String message, int id, Class<?> existing, Class<?> conflicting) {
        super(message);
        this.conflictedId = id;
        this.existingClass = existing;
        this.conflicting = conflicting;
    }

    public static CodecCollisionException forId(int id, Class<?> existing, Class<?> conflicting) {
        String message = String.format(
                "Codec ID Collision Detected! ID [%d] is already occupied. Existing Class: %s, Conflicting Class: %s",
                id, existing.getName(), conflicting.getName());
        return new CodecCollisionException(message, id, existing, conflicting);
    }

    public int getConflictedId() {
        return conflictedId;
    }

    public Class<?> getExistingClass() {
        return existingClass;
    }

    public Class<?> getConflicting() {
        return conflicting;
    }
}
