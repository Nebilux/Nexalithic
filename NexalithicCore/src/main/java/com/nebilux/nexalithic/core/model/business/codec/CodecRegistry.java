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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * {@link PayloadCodec} 的注册、冲突检查和只读查询。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class CodecRegistry<C extends PayloadCodec<?>> {
    private final CodecStorage<C> storage;

    protected CodecRegistry(CodecStorage<C> storage) {
        this.storage = requireNonNull(storage, "storage");
    }

    public final C get(int id) {
        return storage.get(id);
    }

    public final C get(Class<?> type) {
        return storage.get(type);
    }

    public abstract static class Builder<C extends PayloadCodec<?>, SELF extends Builder<C, SELF>> {
        private final Map<Integer, C> byId = new HashMap<>();
        private final Map<Class<?>, C> byClass = new HashMap<>();
        private Function<Map<Integer, C>, ? extends CodecStorage<C>> storageFactory = CodecStorage.ArrayStorage::new;

        protected abstract SELF self();

        public final SELF codec(C codec) {
            requireNonNull(codec, "codec");
            int id = codec.id();
            if (id < 0) {
                throw new IllegalArgumentException("Codec ID must be non-negative: " + id);
            }
            Class<?> type = requireNonNull(codec.clazz(), "type");
            C existing = byId.get(id);
            if (existing != null) {
                throw CodecCollisionException.forId(id, existing.clazz(), type);
            }
            if (byClass.containsKey(type)) {
                throw new IllegalArgumentException("Duplicate Codec type: " + type.getName());
            }
            byId.put(id, codec);
            byClass.put(type, codec);
            return self();
        }

        public final SELF codecs(Collection<? extends C> codecs) {
            requireNonNull(codecs, "codecs").forEach(this::codec);
            return self();
        }

        public final SELF storageFactory(Function<Map<Integer, C>, ? extends CodecStorage<C>> storageFactory) {
            this.storageFactory = requireNonNull(storageFactory, "storageFactory");
            return self();
        }

        protected final CodecStorage<C> buildStorage() {
            return requireNonNull(storageFactory.apply(Map.copyOf(byId)), "storage");
        }
    }
}
