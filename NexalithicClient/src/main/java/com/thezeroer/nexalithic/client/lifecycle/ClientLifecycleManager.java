package com.thezeroer.nexalithic.client.lifecycle;

import com.thezeroer.nexalithic.client.NexalithicClient;
import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.module.ModulesDefinition;
import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.lifecycle.LifecycleManager;

/**
 * 生命周期管理器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/14
 * @version 1.0.0
 */
public class ClientLifecycleManager extends LifecycleManager {
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<GeneralLoop> GeneralLoop = defineModule(GeneralLoop.class);
        private Modules() {
            super(ClientLifecycleManager.class);
        }
    }

    private final GeneralLoop generalLoop;

    public ClientLifecycleManager(NexalithicBuilderContext context) {
        super(NexalithicClient.class.getSimpleName());
        generalLoop = context.getModule(MODULES.GeneralLoop);
    }

    @Override
    public void onStart() {
        generalLoop.start();
    }

    @Override
    public void onStop() {
        generalLoop.stop();
    }

    @Override
    public void onShutdown() {
        generalLoop.shutdown();
    }
}
