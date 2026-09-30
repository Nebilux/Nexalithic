package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverter;
import com.thezeroer.nexalithic.core.model.packet.business.payload.TextPayload;

import java.lang.reflect.Method;

/**
 * 将 Handler 方法返回的 {@link String} 包装为 {@link TextPayload} 响应。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class TextResultConverter<HC extends HandlerContext<?>> extends AbstractBusinessPacketResultConverter<HC> implements HandlerMethodResultConverter<HC> {

    @Override
    public boolean supports(Method method, Class<?> resultType) {
        return resultType == String.class;
    }

    @Override
    public void convert(HC context, Method method, Object result) {
        context.pushResponse(createResponse().attach(new TextPayload((String) result)));
    }

    @Override
    public int priority() {
        return 700;
    }
}
