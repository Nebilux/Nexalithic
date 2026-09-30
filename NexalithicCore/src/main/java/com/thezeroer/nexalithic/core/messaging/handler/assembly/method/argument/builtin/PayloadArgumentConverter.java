package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.annotation.RequestPayload;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverter;
import com.thezeroer.nexalithic.core.model.packet.business.payload.AbstractPayload;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 将被 {@link RequestPayload} 标记的参数解析为请求 Payload 对象本身。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
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
