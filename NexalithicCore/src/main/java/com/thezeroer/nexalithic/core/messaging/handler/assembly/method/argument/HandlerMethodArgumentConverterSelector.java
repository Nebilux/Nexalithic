package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * Handler 方法参数转换器选择器。
 *
 * <p>选择器负责在装配 Handler 方法时，为某一个形参选择合适的
 * {@link HandlerMethodArgumentConverter}。选择器实现可以基于优先级、注解、
 * 参数类型或缓存策略决定返回哪个转换器。</p>
 *
 * <p>该接口只负责选择转换器，不负责执行参数转换。实际转换发生在 Handler 调用期，
 * 由被选中的 {@link HandlerMethodArgumentConverter#convert(HandlerContext, Method, Parameter, int)}
 * 完成。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public interface HandlerMethodArgumentConverterSelector<HC extends HandlerContext<?>> {
    /**
     * 为指定 Handler 方法形参选择转换器。
     *
     * @param method Handler 方法
     * @param parameter 目标形参
     * @param index 形参在方法参数列表中的下标
     * @return 支持该形参的转换器
     * @throws IllegalStateException 没有转换器支持该形参时抛出
     */
    HandlerMethodArgumentConverter<HC> select(Method method, Parameter parameter, int index);
}
