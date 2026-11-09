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

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * {@link PayloadCodec} 注册表的只读存储基类。子类选择 ID 存储方式，共同按具体类型查找。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class CodecStorage<C extends PayloadCodec<?>> {
    private final Map<Class<?>, C> byClass;

    protected CodecStorage(Map<Integer, C> codecs) {
        Map<Integer, C> snapshot = Map.copyOf(requireNonNull(codecs, "codecs"));
        Map<Class<?>, C> classes = new HashMap<>();
        snapshot.forEach((id, codec) -> {
            if (id < 0 || id != codec.id()) {
                throw new IllegalArgumentException("Codec ID does not match registration: " + id);
            }
            Class<?> type = requireNonNull(codec.clazz(), "type");
            if (classes.putIfAbsent(type, codec) != null) {
                throw new IllegalArgumentException("Duplicate Codec type: " + type.getName());
            }
        });
        byClass = Map.copyOf(classes);
    }

    public abstract C get(int id);

    public final C get(Class<?> type) {
        return byClass.get(type);
    }

    /** 适合稀疏分布的类型 ID。 */
    public static final class MapStorage<C extends PayloadCodec<?>> extends CodecStorage<C> {
        private final Map<Integer, C> byId;

        public MapStorage(Map<Integer, C> codecs) {
            super(codecs);
            byId = Map.copyOf(codecs);
        }

        @Override
        public C get(int id) {
            return byId.get(id);
        }
    }

    /** 适合较小且密集的非负类型 ID。 */
    public static final class ArrayStorage<C extends PayloadCodec<?>> extends CodecStorage<C> {
        private static final int MAX_ARRAY_LENGTH = 1 << 20;
        private final Object[] byId;

        public ArrayStorage(Map<Integer, C> codecs) {
            super(codecs);
            Map<Integer, C> snapshot = Map.copyOf(codecs);
            int maxId = snapshot.keySet().stream().mapToInt(Integer::intValue).max().orElse(-1);
            if (maxId >= MAX_ARRAY_LENGTH) {
                throw new IllegalArgumentException("Codec ID is too large for array storage: " + maxId);
            }
            byId = new Object[maxId + 1];
            snapshot.forEach((id, codec) -> byId[id] = codec);
        }

        @Override
        @SuppressWarnings("unchecked")
        public C get(int id) {
            if (id < 0 || id >= byId.length) {
                return null;
            }
            return (C) byId[id];
        }
    }
}
