package com.thezeroer.nexalithic.core.messaging.handler.mapping;

import com.thezeroer.nexalithic.core.builder.NexalithicConfigurer;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

/**
 * Handler 注册表配置器。
 *
 * <p>这是用户手动配置 Handler 注册表的函数式入口，通过
 * {@code NexalithicEndpoint.Builder#handlerRegistryConfigurer(...)} 传入该配置器。</p>
 *
 * <p>配置回调的第一个参数是 {@link HandlerRegistry.Builder}，用于注册手动构建的 Handler
 * 或调整注册表 Trie 子节点存储策略。第二个参数暂未提供辅助对象，固定为 {@code null}，
 * 保留该位置是为了与其他 Nexalithic 配置器入口保持一致。</p>
 *
 * <pre>{@code
 * NexalithicEndpoint.builder()
 *         .handlerRegistryConfigurer((registry, unused) -> {
 *             registry.handler(NexalithicHandler
 *                     .<HandlerContext>builder(pathMatcher, context -> {
 *                         context.pushResponse(response);
 *                     })
 *                     .build());
 *         });
 * }</pre>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/06
 */
@FunctionalInterface
public interface HandlerRegistryConfigurer<HC extends HandlerContext<?>> extends NexalithicConfigurer<HandlerRegistry.Builder<HC>, Void> {
    /**
     * 配置 Handler 注册表构建器。
     *
     * @param builder Handler 注册表构建器
     * @param helper 预留辅助对象参数，当前固定为 {@code null}
     */
    @Override
    void configure(HandlerRegistry.Builder<HC> builder, Void helper);
}
