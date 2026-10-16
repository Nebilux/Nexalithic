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
import com.nebilux.nexalithic.core.model.business.BusinessModel;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 将当前业务请求模型交给类型匹配的 Handler 参数。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class BusinessModelArgumentConverter<HC extends HandlerContext<?>> implements HandlerMethodArgumentConverter<HC> {
    @Override
    public boolean supports(Method method, Parameter parameter, int index) {
        return BusinessModel.class.isAssignableFrom(parameter.getType());
    }

    @Override
    public Object convert(HC context, Method method, Parameter parameter, int index) {
        BusinessModel<?> request = context.getRequest();
        if (!parameter.getType().isInstance(request)) {
            throw new IllegalArgumentException("Request model " + request.getClass().getName()
                    + " does not match parameter " + parameter.getType().getName() + " of " + method.toGenericString());
        }
        return request;
    }

    @Override
    public int priority() {
        return 800;
    }
}
