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
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.business.payload.AbstractPayload;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 基于当前请求 {@link BusinessPacket} 的参数转换器公共基类。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
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
