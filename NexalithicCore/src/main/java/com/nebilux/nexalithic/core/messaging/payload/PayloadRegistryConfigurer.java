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

import com.nebilux.nexalithic.core.builder.NexalithicConfigurer;

/**
 * Payload 注册表配置器。
 *
 * <p>这是用户配置 Payload 构造器注册表的函数式入口，用户通过
 * {@code NexalithicEndpoint.Builder#payloadRegistryConfigurer(...)} 传入该配置器。</p>
 *
 * <p>配置回调会同时取得 {@link PayloadRegistry.Builder} 和 {@link PayloadRegistryHelper}。
 * 前者用于注册 Payload 构造器和设置存储策略；后者提供内置存储策略的 Supplier，
 * 便于调用 {@link PayloadRegistry.Builder#PayloadConstructorStorageSupplier(java.util.function.Supplier)}。</p>
 *
 * <pre>{@code
 * NexalithicEndpoint.builder()
 *         .payloadRegistryConfigurer((registry, helper) -> {
 *             registry.payloadConstructor(LoginPayload::new)
 *                     .payloadConstructor(ErrorPayload::new)
 *                     .PayloadConstructorStorageSupplier(helper.mapPayloadStorageSupplier());
 *         });
 * }</pre>
 *
 * @author Reonvia
 * @since 0.2.0
 */
@FunctionalInterface
public interface PayloadRegistryConfigurer extends NexalithicConfigurer<PayloadRegistry.Builder, PayloadRegistryHelper> {
    /**
     * 配置 Payload 注册表构建器。
     *
     * @param builder Payload 注册表构建器
     * @param helper Payload 注册表辅助对象，提供内置存储策略 Supplier
     */
    @Override
    void configure(PayloadRegistry.Builder builder, PayloadRegistryHelper helper);
}
