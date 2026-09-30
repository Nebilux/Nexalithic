package com.thezeroer.nexalithic.core.messaging.payload;

import com.thezeroer.nexalithic.core.model.packet.business.payload.AbstractPayload;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 有效载荷注册表
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/28
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
        private final Map<Long, Supplier<? extends AbstractPayload<?>>> map = new ConcurrentHashMap<>();
        private Supplier<PayloadConstructorStorage> storageSupplier = PayloadConstructorStorage.ArrayPayloadStorage::new;

        public Builder payloadConstructor(Supplier<? extends AbstractPayload<?>> constructor) {
            AbstractPayload<?> payload = constructor.get();
            if (map.putIfAbsent(payload.getPayloadUID(),  constructor) instanceof AbstractPayload<?> existing) {
                throw new PayloadCollisionException(payload.getPayloadUID(), existing.getClass(), payload.getClass());
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
            return new PayloadRegistry(storageSupplier.get().freeze(map));
        }
    }
}
