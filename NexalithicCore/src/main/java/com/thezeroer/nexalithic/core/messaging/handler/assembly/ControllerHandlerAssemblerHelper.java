package com.thezeroer.nexalithic.core.messaging.handler.assembly;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorAnnotationResolver;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorFactory;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.builtin.DefaultInterceptorAnnotationResolver;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.builtin.DefaultInterceptorFactory;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverterSelector;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.builtin.*;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverterSelector;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.builtin.*;

import java.util.List;
import java.util.function.Consumer;

/**
 * Controller Handler 装配辅助对象。
 *
 * <p>该对象会作为第二个参数传入
 * {@link ControllerHandlerAssemblerConfigurer#configure(ControllerHandlerAssembler.Builder, ControllerHandlerAssemblerHelper)}。
 * 它封装了与当前 Handler 上下文类型相关的默认组件创建逻辑，让用户在配置
 * {@code NexalithicServer.Builder} 或 {@code NexalithicClient.Builder} 时不必手动拼装内置转换器列表。</p>
 *
 * <p>最常用的方法是 {@link #defaultHandlerMethodConverterSelector(ControllerHandlerAssembler.Builder)}，
 * 它会一次性为装配器 Builder 设置默认参数转换器选择器和默认返回值转换器选择器。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class ControllerHandlerAssemblerHelper<HC extends HandlerContext<?>> {
    private final Class<HC> handlerContextType;

    /**
     * 创建指定 Handler 上下文类型的装配辅助对象。
     *
     * <p>通常由 {@code NexalithicServer.Builder} 或 {@code NexalithicClient.Builder}
     * 在调用用户配置器时创建，用户代码不需要直接构造。</p>
     *
     * @param handlerContextType Handler 上下文类型
     */
    public ControllerHandlerAssemblerHelper(Class<HC> handlerContextType) {
        this.handlerContextType = handlerContextType;
    }

    /**
     * 创建默认拦截器注解解析器。
     *
     * <p>当自定义业务注解通过 {@code @InterceptorBinding} 绑定拦截器，并且注解上包含需要解析的配置时，
     * 可以使用该方法创建默认解析器。传入的回调用于继续注册具体注解解析规则。</p>
     *
     * @param builderConsumer 默认解析器构建器配置回调
     * @return 默认拦截器注解解析器
     */
    public InterceptorAnnotationResolver defaultInterceptorAnnotationResolver(Consumer<DefaultInterceptorAnnotationResolver.Builder> builderConsumer) {
        DefaultInterceptorAnnotationResolver.Builder builder = DefaultInterceptorAnnotationResolver.builder();
        builderConsumer.accept(builder);
        return builder.build();
    }

    /**
     * 创建默认拦截器工厂。
     *
     * <p>当 Controller 或 Handler 方法使用了拦截器绑定注解时，需要为装配器配置
     * {@link InterceptorFactory}。传入的回调用于向默认工厂注册具体拦截器创建规则。</p>
     *
     * @param builderConsumer 默认工厂构建器配置回调
     * @return 默认拦截器工厂
     */
    public InterceptorFactory<HC> defaultInterceptorFactory(Consumer<DefaultInterceptorFactory.Builder<HC>> builderConsumer) {
        DefaultInterceptorFactory.Builder<HC> builder = DefaultInterceptorFactory.builder();
        builderConsumer.accept(builder);
        return builder.build();
    }

    /**
     * 创建默认 Handler 方法参数转换器选择器，并允许追加自定义参数转换器。
     *
     * <p>传入的回调会在默认转换器列表创建后执行，
     * 可用于追加更高优先级或业务特定的参数转换器。</p>
     *
     * @param builderConsumer 默认参数转换器选择器构建器配置回调
     * @return 默认 Handler 方法参数转换器选择器
     */
    public HandlerMethodArgumentConverterSelector<HC> defaultHandlerMethodArgumentConverterSelector(Consumer<DefaultHandlerMethodArgumentConverterSelector.Builder<HC>> builderConsumer) {
        DefaultHandlerMethodArgumentConverterSelector.Builder<HC> builder = defaultHandlerMethodArgumentConverterSelectorBuilder();
        builderConsumer.accept(builder);
        return builder.build();
    }

    /**
     * 创建默认 Handler 方法参数转换器选择器。
     *
     * @return 默认 Handler 方法参数转换器选择器
     */
    public HandlerMethodArgumentConverterSelector<HC> defaultHandlerMethodArgumentConverterSelector() {
        return defaultHandlerMethodArgumentConverterSelectorBuilder().build();
    }

    /**
     * 创建默认 Handler 方法返回值转换器选择器，并允许追加自定义返回值转换器。
     *
     * <p>传入的回调会在默认转换器列表创建后执行，
     * 可用于追加更高优先级或业务特定的返回值转换器。</p>
     *
     * @param builderConsumer 默认返回值转换器选择器构建器配置回调
     * @return 默认 Handler 方法返回值转换器选择器
     */
    public HandlerMethodResultConverterSelector<HC> defaultHandlerMethodResultConverterSelector(Consumer<DefaultHandlerMethodResultConverterSelector.Builder<HC>> builderConsumer) {
        DefaultHandlerMethodResultConverterSelector.Builder<HC> builder = defaultHandlerMethodResultConverterSelectorBuilder();
        builderConsumer.accept(builder);
        return builder.build();
    }

    /**
     * 创建默认 Handler 方法返回值转换器选择器。
     *
     * @return 默认 Handler 方法返回值转换器选择器
     */
    public HandlerMethodResultConverterSelector<HC> defaultHandlerMethodResultConverterSelector() {
        return defaultHandlerMethodResultConverterSelectorBuilder().build();
    }

    /**
     * 为装配器 Builder 同时启用默认参数转换器和默认返回值转换器。
     *
     * <p>这是用户配置注解式 Controller 时最常用的快捷方法。返回值仍然是传入的装配器 Builder，
     * 因此可以继续链式调用 {@link ControllerHandlerAssembler.Builder#controller(Object)}
     * 或 {@link ControllerHandlerAssembler.Builder#scanControllers(String, com.thezeroer.nexalithic.core.util.BeanFactory)}。</p>
     *
     * <pre>{@code
     * nexalithicBuilder.controllerHandlerAssemblerConfigurer((builder, helper) -> {
     *     helper.defaultHandlerMethodConverterSelector(builder)
     *             .controller(new AuthController());
     * });
     * }</pre>
     *
     * @param builder Controller Handler 装配器构建器
     * @return 已配置默认方法转换器的同一个构建器
     */
    public ControllerHandlerAssembler.Builder<HC> defaultHandlerMethodConverterSelector(ControllerHandlerAssembler.Builder<HC> builder) {
        return builder.argumentConverterSelector(defaultHandlerMethodArgumentConverterSelector()).resultConverterSelector(defaultHandlerMethodResultConverterSelector());
    }

    private DefaultHandlerMethodArgumentConverterSelector.Builder<HC> defaultHandlerMethodArgumentConverterSelectorBuilder() {
        return DefaultHandlerMethodArgumentConverterSelector.<HC>builder().converters(List.of(
                new HandlerContextArgumentConverter<>(handlerContextType),
                new BusinessPacketArgumentConverter<>(),
                new BusinessPacketPathArgumentConverter<>(),
                new BusinessPacketWayArgumentConverter<>(),
                new PayloadArgumentConverter<>()
        ));
    }

    private DefaultHandlerMethodResultConverterSelector.Builder<HC> defaultHandlerMethodResultConverterSelectorBuilder() {
        return DefaultHandlerMethodResultConverterSelector.<HC>builder().converters(List.of(
                new BusinessPacketResultConverter<>(),
                new PayloadResultConverter<>(),
                new TextResultConverter<>(),
                new FileResultConverter<>()
        ));
    }
}
