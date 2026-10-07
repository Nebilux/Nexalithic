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

import com.nebilux.nexalithic.core.exception.NexalithicException;

/**
 * 当多个 Payload 类产生相同的 IdentityCode 时抛出。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class PayloadCollisionException extends NexalithicException {
    private final long conflictedId;
    private final Class<?> existingClass;
    private final Class<?> conflicting;

    public PayloadCollisionException(long id, Class<?> existing, Class<?> conflicting) {
        super(String.format(
                "Payload ID Collision Detected! ID [%d] is already occupied. Existing Class: %s, Conflicting Class: %s",
                id, existing.getName(), conflicting.getName()
        ));
        this.conflictedId = id;
        this.existingClass = existing;
        this.conflicting = conflicting;
    }

    public long getConflictedId() { return conflictedId; }
    public Class<?> getExistingClass() { return existingClass; }
    public Class<?> getConflicting() { return conflicting; }
}
