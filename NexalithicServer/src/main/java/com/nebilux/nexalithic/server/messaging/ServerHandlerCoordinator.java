package com.nebilux.nexalithic.server.messaging;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.lifecycle.ServerLifecycleManager;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.session.ServerSession;
import com.nebilux.nexalithic.server.manager.SessionsManager;

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
    private final SessionsManager sessionsManager;

    public ServerHandlerCoordinator(NexalithicBuilderContext context) {
        super(context, OPTIONS, context.getOption(ServerLifecycleManager.OPTIONS.ServiceUnit_Count) != 1 || context.getOption(ServiceUnit.OPTIONS.BusinessLoop_Count) != 1);
        sessionsManager = context.getModule(NexalithicServer.MODULES.SessionsManager);
        init(context, OPTIONS);
    }

    @Override
    protected ServerHandlerContext.Recyclable createRecyclableWrapper(GenericWrapperPool<ServerHandlerContext, ServerHandlerContext.Recyclable> owner) {
        return new ServerHandlerContext.Recyclable(owner, new ServerHandlerContext(sessionsManager));
    }
}
