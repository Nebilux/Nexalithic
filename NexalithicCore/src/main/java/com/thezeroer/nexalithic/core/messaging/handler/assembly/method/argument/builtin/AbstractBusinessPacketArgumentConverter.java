package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.model.packet.business.payload.AbstractPayload;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 基于当前请求 {@link BusinessPacket} 的参数转换器公共基类。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
abstract class AbstractBusinessPacketArgumentConverter<HC extends HandlerContext<?>> {

    /**
     * 取得当前上下文绑定的请求包。
     *
     * @param context 当前 Handler 上下文
     * @param method 当前 Handler 方法
     * @return 当前请求包
     */
    protected final BusinessPacket request(HC context, Method method) {
        BusinessPacket request = context.getRequest();
        if (request == null) {
            throw new IllegalStateException(
                    "No BusinessPacket request is bound to handler context for method "
                            + method.toGenericString()
            );
        }
        return request;
    }

    /**
     * 取得当前请求包中指定下标的 Payload。
     *
     * @param context 当前 Handler 上下文
     * @param method 当前 Handler 方法
     * @param parameter 当前目标形参
     * @param payloadIndex Payload 下标
     * @return 指定下标的 Payload
     */
    protected final AbstractPayload<?> payload(HC context, Method method, Parameter parameter, int payloadIndex) {
        if (payloadIndex < 0) {
            throw new IllegalArgumentException(
                    "Request payload index must not be negative for parameter '%s' of method %s"
                            .formatted(parameter.getName(), method.toGenericString())
            );
        }
        AbstractPayload<?> payload = request(context, method).payload(payloadIndex);
        if (payload == null) {
            throw new IllegalArgumentException(
                    "No request payload at index %d for parameter '%s' of method %s"
                            .formatted(payloadIndex, parameter.getName(), method.toGenericString())
            );
        }
        return payload;
    }
}
