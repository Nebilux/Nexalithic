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
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;

/**
 * 客户端业务分发器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClientHandlerCoordinator extends HandlerCoordinator<
        ClientSession,
        ClientHandlerContext,
        ClientHandlerContext.Recyclable
        > {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, ClientHandlerCoordinator.class);
    public static final class Options extends HandlerCoordinator.Options {
        private Options(Class<?> holder) {
            super(holder);
        }
        protected Integer HandlerContextPool_Capacity_DefaultValue() {
            return 4;
        }
    }

    public ClientHandlerCoordinator(NexalithicBuilderContext context) {
        super(context, OPTIONS, false);
        init(context, OPTIONS);
    }

    @Override
    protected ClientHandlerContext.Recyclable createRecyclableWrapper(GenericWrapperPool<ClientHandlerContext, ClientHandlerContext.Recyclable> owner) {
        return new ClientHandlerContext.Recyclable(owner, new ClientHandlerContext());
    }
}
