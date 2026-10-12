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

package com.nebilux.nexalithic.core.messaging.handler.assembly.method.result.builtin;

import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;
import com.nebilux.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverter;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;

import java.lang.reflect.Method;

/**
 * 将 Handler 方法返回的 {@link BusinessPacket} 作为响应包推送。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
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
