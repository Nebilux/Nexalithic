package com.thezeroer.nexalithic.core.messaging.payload;

import com.thezeroer.nexalithic.core.builder.NexalithicConfigurer;

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
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/06
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
