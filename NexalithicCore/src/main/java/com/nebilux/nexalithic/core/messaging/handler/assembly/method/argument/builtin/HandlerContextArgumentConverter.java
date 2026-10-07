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

package com.nebilux.nexalithic.core.messaging.handler.assembly.method.argument.builtin;

import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;
import com.nebilux.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverter;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Objects;

/**
 * 将当前 {@link HandlerContext} 解析为 Handler 方法参数。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
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
