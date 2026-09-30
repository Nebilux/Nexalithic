package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverter;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Objects;

/**
 * 将当前 {@link HandlerContext} 解析为 Handler 方法参数。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class HandlerContextArgumentConverter<HC extends HandlerContext<?>> implements HandlerMethodArgumentConverter<HC> {
    private final Class<HC> handlerContextType;

    /**
     * 创建 Handler 上下文参数转换器。
     *
     * @param handlerContextType 装配器运行时提供的 Handler 上下文类型
     */
    public HandlerContextArgumentConverter(Class<HC> handlerContextType) {
        this.handlerContextType = Objects.requireNonNull(handlerContextType, "handlerContextType");
    }

    @Override
    public boolean supports(Method method, Parameter parameter, int index) {
        Class<?> parameterType = parameter.getType();
        return HandlerContext.class.isAssignableFrom(parameterType) && parameterType.isAssignableFrom(handlerContextType);
    }

    @Override
    public Object convert(HC context, Method method, Parameter parameter, int index) {
        return context;
    }

    @Override
    public int priority() {
        return 1000;
    }
}
