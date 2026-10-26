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

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * {@link ChunkCodec} 的只读索引。构造时固定注册内容，之后支持并发读取。
 * 自定义实现必须根据 ID 和块的具体类返回同一个 Codec。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface ChunkCodecStorage {
    ChunkCodec<?> get(int id);

    ChunkCodec<?> get(Class<? extends BusinessStream.Chunk> chunkType);

    private static Map<Class<? extends BusinessStream.Chunk>, ChunkCodec<?>> classIndex(Map<Integer, ChunkCodec<?>> codecs) {
        Map<Class<? extends BusinessStream.Chunk>, ChunkCodec<?>> byClass = new HashMap<>();
        codecs.forEach((id, codec) -> {
            requireNonNull(id, "id");
            requireNonNull(codec, "codec");
            if (id < 0 || id != codec.id()) {
                throw new IllegalArgumentException("ChunkCodec ID does not match registration: " + id);
            }
            Class<? extends BusinessStream.Chunk> type = requireNonNull(codec.clazz(), "chunkType");
            if (byClass.putIfAbsent(type, codec) != null) {
                throw new IllegalArgumentException("Duplicate ChunkCodec type: " + type.getName());
            }
        });
        return Map.copyOf(byClass);
    }

    /**
     * 适合稀疏分布的类型 ID。
     *
     * @author Reonvia
     * @since 0.5.0
     */
    final class MapStorage implements ChunkCodecStorage {
        private final Map<Integer, ChunkCodec<?>> byId;
        private final Map<Class<? extends BusinessStream.Chunk>, ChunkCodec<?>> byClass;

        public MapStorage(Map<Integer, ChunkCodec<?>> codecs) {
            Map<Integer, ChunkCodec<?>> snapshot = Map.copyOf(requireNonNull(codecs, "codecs"));
            this.byId = snapshot;
            this.byClass = classIndex(snapshot);
        }

        @Override
        public ChunkCodec<?> get(int id) {
            return byId.get(id);
        }

        @Override
        public ChunkCodec<?> get(Class<? extends BusinessStream.Chunk> chunkType) {
            return byClass.get(chunkType);
        }

    }

    /**
     * 适合较小且密集的非负类型 ID。
     *
     * @author Reonvia
     * @since 0.5.0
     */
    final class ArrayStorage implements ChunkCodecStorage {
        private static final int MAX_ARRAY_LENGTH = 1 << 20;

        private final ChunkCodec<?>[] byId;
        private final Map<Class<? extends BusinessStream.Chunk>, ChunkCodec<?>> byClazz;

        public ArrayStorage(Map<Integer, ChunkCodec<?>> codecs) {
            Map<Integer, ChunkCodec<?>> snapshot = Map.copyOf(requireNonNull(codecs, "codecs"));
            Map<Class<? extends BusinessStream.Chunk>, ChunkCodec<?>> classes = classIndex(snapshot);
            int maxId = snapshot.keySet().stream().mapToInt(Integer::intValue).max().orElse(-1);
            if (maxId >= MAX_ARRAY_LENGTH) {
                throw new IllegalArgumentException("ChunkCodec ID is too large for array storage: " + maxId);
            }
            ChunkCodec<?>[] table = new ChunkCodec<?>[maxId + 1];
            snapshot.forEach((id, codec) -> table[id] = codec);
            this.byId = table;
            this.byClazz = classes;
        }

        @Override
        public ChunkCodec<?> get(int id) {
            return id < 0 || id >= byId.length ? null : byId[id];
        }

        @Override
        public ChunkCodec<?> get(Class<? extends BusinessStream.Chunk> chunkType) {
            return byClazz.get(chunkType);
        }

    }
}
