package com.nebilux.nexalithic.client.lifecycle;

import com.nebilux.nexalithic.client.NexalithicClient;
import com.nebilux.nexalithic.client.io.session.ClientSessionLoop;
import com.nebilux.nexalithic.client.session.SessionManager;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.lifecycle.LifecycleCoordinator;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;

import java.util.concurrent.CompletionStage;

/**
 * 客户端生命周期协调器。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClientLifecycleCoordinator extends LifecycleCoordinator {
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<ClientSessionLoop> SessionLoop = defineModule(ClientSessionLoop.class);
        private Modules() {
            super(ClientLifecycleCoordinator.class);
        }
    }

    private final ClientSessionLoop sessionLoop;
    private final SessionManager sessionManager;
    private final TaskScheduler taskScheduler;

    public ClientLifecycleCoordinator(NexalithicBuilderContext context) {
        super(NexalithicClient.class.getSimpleName());
        sessionLoop = context.getModule(MODULES.SessionLoop);
        sessionManager = context.getModule(NexalithicClient.MODULES.SessionManager);
        taskScheduler = context.getModule(NexalithicClient.MODULES.TaskScheduler);
    }

    @Override
    protected CompletionStage<Void> onStart() {
        sessionManager.start();
        return sessionLoop.start().whenComplete((ignored, failure) -> {
            if (failure != null) {
                sessionManager.stop();
            }
        });
    }

    @Override
    protected CompletionStage<Void> onStop() {
        sessionManager.stop();
        return sessionLoop.stop().whenComplete((ignored, failure) -> taskScheduler.shutdown());
    }

    @Override
    protected CompletionStage<Void> onShutdown() {
        sessionManager.shutdown();
        return sessionLoop.shutdown().whenComplete((ignored, failure) -> {
            try {
                sessionManager.stop();
            } finally {
                taskScheduler.shutdown();
            }
        });
    }
}
