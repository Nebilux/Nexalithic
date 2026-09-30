package com.thezeroer.nexalithic.core.messaging.handler.assembly;

import com.thezeroer.nexalithic.core.builder.NexalithicConfigurer;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

/**
 * Controller Handler 装配器配置器。
 *
 * <p>这是用户配置注解式 Controller 的函数式入口，用户通过
 * {@code NexalithicEndpoint.Builder#controllerHandlerAssemblerConfigurer(...)} 传入该配置器。</p>
 *
 * <p>配置回调会同时取得 {@link ControllerHandlerAssembler.Builder} 和
 * {@link ControllerHandlerAssemblerHelper}。前者用于注册 Controller、拦截器组件和转换器选择器；
 * 后者用于快速创建默认转换器或默认拦截器相关组件。</p>
 *
 * <pre>{@code
 * NexalithicEndpoint.builder()
 *         .controllerHandlerAssemblerConfigurer((builder, helper) -> {
 *             helper.defaultHandlerMethodConverterSelector(builder)
 *                     .controller(new UserController());
 *         });
 * }</pre>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @since 2026/08/02
 * @version 1.0.0
 */
@FunctionalInterface
public interface ControllerHandlerAssemblerConfigurer<HC extends HandlerContext<?>> extends NexalithicConfigurer<ControllerHandlerAssembler.Builder<HC>, ControllerHandlerAssemblerHelper<HC>> {
    /**
     * 配置 Controller Handler 装配器构建器。
     *
     * @param builder Controller Handler 装配器构建器
     * @param helper Controller Handler 装配辅助对象
     */
    @Override
    void configure(ControllerHandlerAssembler.Builder<HC> builder, ControllerHandlerAssemblerHelper<HC> helper);
}
