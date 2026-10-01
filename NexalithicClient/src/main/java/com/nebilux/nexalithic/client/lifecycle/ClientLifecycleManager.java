package com.nebilux.nexalithic.client.lifecycle;

import com.nebilux.nexalithic.client.NexalithicClient;
import com.nebilux.nexalithic.client.io.session.ClientSessionLoop;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.lifecycle.LifecycleManager;

/**
 * 生命周期管理器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClientLifecycleManager extends LifecycleManager {
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<ClientSessionLoop> SessionLoop = defineModule(ClientSessionLoop.class);
        private Modules() {
            super(ClientLifecycleManager.class);
        }
    }

    private final ClientSessionLoop sessionLoop;

    public ClientLifecycleManager(NexalithicBuilderContext context) {
        super(NexalithicClient.class.getSimpleName());
        sessionLoop = context.getModule(MODULES.SessionLoop);
    }

    @Override
    public void onStart() {
        sessionLoop.start();
    }

    @Override
    public void onStop() {
        sessionLoop.stop();
    }

    @Override
    public void onShutdown() {
        sessionLoop.shutdown();
    }
}
