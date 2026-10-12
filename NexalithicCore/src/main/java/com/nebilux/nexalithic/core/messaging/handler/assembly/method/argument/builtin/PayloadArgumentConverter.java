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
import com.nebilux.nexalithic.core.messaging.handler.assembly.method.argument.annotation.RequestPayload;
import com.nebilux.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverter;
import com.nebilux.nexalithic.core.model.packet.business.payload.AbstractPayload;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 将被 {@link RequestPayload} 标记的参数解析为请求 Payload 对象本身。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class PayloadArgumentConverter<HC extends HandlerContext<?>> extends AbstractBusinessPacketArgumentConverter<HC> implements HandlerMethodArgumentConverter<HC> {

    @Override
    public boolean supports(Method method, Parameter parameter, int index) {
        return parameter.isAnnotationPresent(RequestPayload.class) && AbstractPayload.class.isAssignableFrom(parameter.getType());
    }

    @Override
    public Object convert(HC context, Method method, Parameter parameter, int index) {
        RequestPayload annotation = parameter.getAnnotation(RequestPayload.class);
        AbstractPayload<?> payload = payload(context, method, parameter, annotation.value());
        if (!parameter.getType().isInstance(payload)) {
            throw new IllegalArgumentException(
                    "Request payload at index %d is '%s' but parameter '%s' requires '%s' in method %s"
                            .formatted(
                                    annotation.value(),
                                    payload.getClass().getName(),
                                    parameter.getName(),
                                    parameter.getType().getName(),
                                    method.toGenericString()
                            )
            );
        }
        return payload;
    }

    @Override
    public int priority() {
        return 800;
    }
}
