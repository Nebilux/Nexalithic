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

import java.util.Map;
import java.util.function.Function;

/**
 * 提供内置 ChunkCodec 存储策略的构造函数。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public final class ChunkCodecRegistryHelper {
    public static final ChunkCodecRegistryHelper INSTANCE = new ChunkCodecRegistryHelper();

    private ChunkCodecRegistryHelper() {}

    /** 适合类型 ID 稀疏分布的 Map 存储策略。 */
    public Function<Map<Integer, ChunkCodec<?>>, ChunkCodecStorage> mapStorageFactory() {
        return ChunkCodecStorage.MapStorage::new;
    }

    /** 适合较小且密集的类型 ID 的数组存储策略。 */
    public Function<Map<Integer, ChunkCodec<?>>, ChunkCodecStorage> arrayStorageFactory() {
        return ChunkCodecStorage.ArrayStorage::new;
    }
}
