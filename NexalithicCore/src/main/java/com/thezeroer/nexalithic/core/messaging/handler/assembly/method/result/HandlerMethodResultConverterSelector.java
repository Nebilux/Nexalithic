package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

import java.lang.reflect.Method;

/**
 * Handler 方法结果转换器选择器。
 *
 * <p>选择器负责在装配 Handler 方法时，为方法返回类型选择合适的
 * {@link HandlerMethodResultConverter}。选择器实现可以基于优先级、返回类型、
 * 方法注解或缓存策略决定返回哪个转换器。</p>
 *
 * <p>该接口只负责选择转换器，不负责执行结果转换。实际处理发生在 Handler 调用期，
 * 由被选中的 {@link HandlerMethodResultConverter#convert(HandlerContext, Method, Object)}
 * 完成。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public interface HandlerMethodResultConverterSelector<HC extends HandlerContext<?>> {
    /**
     * 为指定 Handler 方法返回类型选择转换器。
     *
     * @param method Handler 方法
     * @return 支持该返回类型的转换器
     * @throws IllegalStateException 没有转换器支持该返回类型时抛出
     */
    HandlerMethodResultConverter<HC> select(Method method);
}
