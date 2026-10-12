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

import com.nebilux.nexalithic.core.model.packet.business.payload.AbstractPayload;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 有效载荷注册表
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class PayloadRegistry {
    private final PayloadConstructorStorage constructors;

    private PayloadRegistry(PayloadConstructorStorage constructors) {
        this.constructors = constructors;
    }

    public static Builder builder() {
        return new Builder();
    }

    public AbstractPayload<?> get(long UID) {
        Supplier<? extends AbstractPayload<?>> constructor = constructors.get(UID);
        if (constructor == null) {
            return null;
        }
        return constructor.get();
    }

    public static class Builder {
        private record Registration(Supplier<? extends AbstractPayload<?>> constructor, Class<?> payloadType) {}

        private final Map<Long, Registration> map = new ConcurrentHashMap<>();
        private Supplier<PayloadConstructorStorage> storageSupplier = PayloadConstructorStorage.ArrayPayloadStorage::new;

        public Builder payloadConstructor(Supplier<? extends AbstractPayload<?>> constructor) {
            AbstractPayload<?> payload = constructor.get();
            long uid = payload.getPayloadUID();
            Registration existing = map.putIfAbsent(uid, new Registration(constructor, payload.getClass()));
            if (existing != null) {
                throw PayloadCollisionException.forId(uid, existing.payloadType(), payload.getClass());
            }
            return this;
        }

        public Builder payloadConstructors(Collection<Supplier<? extends AbstractPayload<?>>> constructors) {
            for (Supplier<? extends AbstractPayload<?>> constructor : constructors) {
                payloadConstructor(constructor);
            }
            return this;
        }

        public Builder PayloadConstructorStorageSupplier(Supplier<PayloadConstructorStorage> storageSupplier) {
            this.storageSupplier = storageSupplier;
            return this;
        }


        public PayloadRegistry build() {
            Map<Long, Supplier<? extends AbstractPayload<?>>> constructors = new HashMap<>();
            map.forEach((uid, registration) -> constructors.put(uid, registration.constructor()));
            return new PayloadRegistry(storageSupplier.get().freeze(constructors));
        }
    }
}
