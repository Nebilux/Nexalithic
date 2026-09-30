package com.thezeroer.nexalithic.server.messaging;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.thezeroer.nexalithic.server.NexalithicServer;
import com.thezeroer.nexalithic.server.lifecycle.ServerLifecycleManager;
import com.thezeroer.nexalithic.server.io.session.ServiceUnit;
import com.thezeroer.nexalithic.server.session.ServerSession;
import com.thezeroer.nexalithic.server.manager.SessionsManager;

/**
 * 服务器业务分组器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/17
 * @version 1.0.0
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
