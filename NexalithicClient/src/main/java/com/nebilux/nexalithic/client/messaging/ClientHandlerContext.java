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

package com.nebilux.nexalithic.client.messaging;

import com.nebilux.nexalithic.client.session.ClientSession;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;

/**
 * 客户端处理器上下文
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClientHandlerContext extends HandlerContext<ClientSession> {

    public ClientHandlerContext() {}

    public static class Recyclable extends HandlerContext.Recyclable<
            ClientSession,
            ClientHandlerContext,
            Recyclable
        > {
        public Recyclable(GenericWrapperPool<ClientHandlerContext, Recyclable> owner, ClientHandlerContext target) {
            super(owner, target);
        }
    }
}
