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

import java.util.function.Supplier;

/**
 * Payload 注册表配置辅助对象。
 *
 * <p>该对象会作为第二个参数传入 {@link PayloadRegistryConfigurer#configure(PayloadRegistry.Builder, PayloadRegistryHelper)}。
 * 当前辅助对象只提供内置 {@link PayloadConstructorStorage} 实现的 Supplier，供
 * {@link PayloadRegistry.Builder#PayloadConstructorStorageSupplier(Supplier)} 使用。
 * Payload 构造器注册应直接通过 {@link PayloadRegistry.Builder#payloadConstructor(Supplier)}
 * 或 {@link PayloadRegistry.Builder#payloadConstructors(java.util.Collection)} 完成。</p>
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class PayloadRegistryHelper {
    /**
     * 共享的 Payload 注册表辅助对象。
     */
    public static final PayloadRegistryHelper INSTANCE = new PayloadRegistryHelper();

    private PayloadRegistryHelper() {}

    /**
     * 返回数组型 Payload 构造器存储策略的 Supplier。
     *
     * <p>适合 Payload UID 较连续、最大 UID 不大的场景。Server 和 Client Builder 默认使用该策略。</p>
     *
     * @return 数组型 Payload 构造器存储策略 Supplier
     */
    public Supplier<PayloadConstructorStorage> arrayPayloadStorageSupplier() {
        return PayloadConstructorStorage.ArrayPayloadStorage::new;
    }

    /**
     * 返回 Map 型 Payload 构造器存储策略的 Supplier。
     *
     * <p>适合 Payload UID 较稀疏或最大 UID 较大的场景。</p>
     *
     * @return Map 型 Payload 构造器存储策略 Supplier
     */
    public Supplier<PayloadConstructorStorage> mapPayloadStorageSupplier() {
        return PayloadConstructorStorage.MapPayloadStorage::new;
    }
}
