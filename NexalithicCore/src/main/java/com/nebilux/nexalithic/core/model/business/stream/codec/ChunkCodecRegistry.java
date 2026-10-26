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

package com.nebilux.nexalithic.core.model.business.stream.codec;

import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * 块编解码器注册表
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class ChunkCodecRegistry {
    private final ChunkCodecStorage codecs;

    private ChunkCodecRegistry(ChunkCodecStorage codecs) {
        this.codecs = codecs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public ChunkCodec<?> get(int id) {
        return codecs.get(id);
    }

    public ChunkCodec<?> get(Class<? extends BusinessStream.Chunk> type) {
        return codecs.get(type);
    }

    public static final class Builder {
        private final Map<Integer, ChunkCodec<?>> byId = new HashMap<>();
        private final Map<Class<? extends BusinessStream.Chunk>, ChunkCodec<?>> byClazz = new HashMap<>();
        private Function<Map<Integer, ChunkCodec<?>>, ? extends ChunkCodecStorage> storageFactory = ChunkCodecStorage.ArrayStorage::new;

        public Builder chunkCodec(ChunkCodec<?> codec) {
            requireNonNull(codec, "codec");
            int id = codec.id();
            if (id < 0) {
                throw new IllegalArgumentException("ChunkCodec ID must be non-negative: " + id);
            }
            Class<? extends BusinessStream.Chunk> clazz = requireNonNull(codec.clazz(), "clazz");
            if (byId.containsKey(id)) {
                throw new IllegalArgumentException("Duplicate ChunkCodec ID: " + id);
            }
            if (byClazz.containsKey(clazz)) {
                throw new IllegalArgumentException("Duplicate ChunkCodec Clazz: " + clazz.getName());
            }
            byId.put(id, codec);
            byClazz.put(clazz, codec);
            return this;
        }

        public Builder chunkCodecs(Collection<? extends ChunkCodec<?>> codecs) {
            requireNonNull(codecs, "codecs").forEach(this::chunkCodec);
            return this;
        }

        public Builder storageFactory(Function<Map<Integer, ChunkCodec<?>>, ? extends ChunkCodecStorage> storageFactory) {
            this.storageFactory = requireNonNull(storageFactory, "storageFactory");
            return this;
        }

        public ChunkCodecRegistry build() {
            return new ChunkCodecRegistry(requireNonNull(storageFactory.apply(Map.copyOf(byId)), "storage"));
        }
    }
}
