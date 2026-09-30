package com.nebilux.nexalithic.client.messaging;

import com.nebilux.nexalithic.client.session.ClientSession;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;

/**
 * 客户端业务分发器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/18
 * @version 1.0.0
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
