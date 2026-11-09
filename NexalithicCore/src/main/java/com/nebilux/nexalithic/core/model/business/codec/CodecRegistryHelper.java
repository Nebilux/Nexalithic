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

import java.util.Map;
import java.util.function.Function;

/**
 * 为 Codec 注册表提供 Map 与数组存储策略。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public final class CodecRegistryHelper {
    public static final CodecRegistryHelper INSTANCE = new CodecRegistryHelper();

    private CodecRegistryHelper() {
    }

    public <C extends PayloadCodec<?>> Function<Map<Integer, C>, CodecStorage<C>> mapStorageFactory() {
        return CodecStorage.MapStorage::new;
    }

    public <C extends PayloadCodec<?>> Function<Map<Integer, C>, CodecStorage<C>> arrayStorageFactory() {
        return CodecStorage.ArrayStorage::new;
    }
}
