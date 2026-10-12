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
import com.nebilux.nexalithic.core.messaging.handler.assembly.method.argument.annotation.RequestPath;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 将当前请求包路径解析为防御性复制后的 {@code short[]} 参数。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class BusinessPacketPathArgumentConverter<HC extends HandlerContext<?>> extends AbstractBusinessPacketArgumentConverter<HC> implements HandlerMethodArgumentConverter<HC> {

    @Override
    public boolean supports(Method method, Parameter parameter, int index) {
        return parameter.isAnnotationPresent(RequestPath.class) && parameter.getType() == short[].class;
    }

    @Override
    public Object convert(HC context, Method method, Parameter parameter, int index) {
        short[] path = request(context, method).getPath();
        return path == null ? null : path.clone();
    }

    @Override
    public int priority() {
        return 900;
    }
}
