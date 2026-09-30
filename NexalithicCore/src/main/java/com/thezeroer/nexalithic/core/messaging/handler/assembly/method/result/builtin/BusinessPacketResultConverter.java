package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverter;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;

import java.lang.reflect.Method;

/**
 * 将 Handler 方法返回的 {@link BusinessPacket} 作为响应包推送。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class BusinessPacketResultConverter<HC extends HandlerContext<?>> implements HandlerMethodResultConverter<HC> {

    @Override
    public boolean supports(Method method, Class<?> resultType) {
        return BusinessPacket.class.isAssignableFrom(resultType);
    }

    @Override
    public void convert(HC context, Method method, Object result) {
        context.pushResponse((BusinessPacket) result);
    }

    @Override
    public int priority() {
        return 1000;
    }
}
