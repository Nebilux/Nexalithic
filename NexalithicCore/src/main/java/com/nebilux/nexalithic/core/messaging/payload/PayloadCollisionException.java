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

package com.nebilux.nexalithic.core.messaging.payload;

import com.nebilux.nexalithic.core.exception.NexalithicConflictException;

/**
 * 载荷标识在注册时发生冲突的异常。
 *
 * <p>新注册的载荷试图占用已登记的载荷 ID 时产生，即使两次注册的是同一类型；
 * 冲突 ID、已登记类型和新类型分别保留在异常中。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class PayloadCollisionException extends NexalithicConflictException {
    private final long conflictedId;
    private final Class<?> existingClass;
    private final Class<?> conflicting;

    private PayloadCollisionException(String message, long id, Class<?> existing, Class<?> conflicting) {
        super(message);
        this.conflictedId = id;
        this.existingClass = existing;
        this.conflicting = conflicting;
    }

    public static PayloadCollisionException forId(long id, Class<?> existing, Class<?> conflicting) {
        String message = String.format(
                "Payload ID Collision Detected! ID [%d] is already occupied. Existing Class: %s, Conflicting Class: %s",
                id, existing.getName(), conflicting.getName());
        return new PayloadCollisionException(message, id, existing, conflicting);
    }

    public long getConflictedId() { return conflictedId; }
    public Class<?> getExistingClass() { return existingClass; }
    public Class<?> getConflicting() { return conflicting; }
}
