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

package com.nebilux.nexalithic.server.messaging;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.lifecycle.ServerLifecycleCoordinator;
import com.nebilux.nexalithic.server.session.ServerSession;
import com.nebilux.nexalithic.server.session.SessionRegistry;

/**
 * 服务器业务分组器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ServerHandlerCoordinator extends HandlerCoordinator<
        ServerSession,
        ServerHandlerContext,
        ServerHandlerContext.Recyclable
        > {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, ServerHandlerCoordinator.class);
    public static final class Options extends HandlerCoordinator.Options {
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    private final SessionRegistry sessionRegistry;

    public ServerHandlerCoordinator(NexalithicBuilderContext context) {
        super(context, OPTIONS, context.getOption(ServerLifecycleCoordinator.OPTIONS.ServiceUnit_Count) != 1 || context.getOption(ServiceUnit.OPTIONS.BusinessLoop_Count) != 1);
        sessionRegistry = context.getModule(NexalithicServer.MODULES.SessionRegistry);
        init(context, OPTIONS);
    }

    @Override
    protected ServerHandlerContext.Recyclable createRecyclableWrapper(GenericWrapperPool<ServerHandlerContext, ServerHandlerContext.Recyclable> owner) {
        return new ServerHandlerContext.Recyclable(owner, new ServerHandlerContext(sessionRegistry));
    }
}
