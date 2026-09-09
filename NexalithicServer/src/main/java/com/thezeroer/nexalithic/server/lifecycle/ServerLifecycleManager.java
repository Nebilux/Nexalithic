package com.thezeroer.nexalithic.server.lifecycle;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.module.ModulesDefinition;
import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.thezeroer.nexalithic.core.lifecycle.LifecycleManager;
import com.thezeroer.nexalithic.server.NexalithicServer;
import com.thezeroer.nexalithic.server.lifecycle.accept.AcceptorLoop;
import com.thezeroer.nexalithic.server.lifecycle.handshake.HandshakeLoop;
import com.thezeroer.nexalithic.server.lifecycle.service.ServiceUnit;
import com.thezeroer.nexalithic.server.lifecycle.service.WorkerLoop;

/**
 * 生命周期管理器
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/02/19
 */
public class ServerLifecycleManager extends LifecycleManager {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, ServerLifecycleManager.class);
    public static final class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> HandshakeLoop_Count = defineOption(
                1, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> ServiceUnit_Count = defineOption(
                1, OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<AcceptorLoop> AcceptorLoop = defineModule(AcceptorLoop.class);
        public final NexalithicModule<LoadBalancer<Void, HandshakeLoop>> HandshakeLoopLoadBalancer = defineModule(LoadBalancer.class, HandshakeLoop.class.getSimpleName());
        public final NexalithicModule<LoadBalancer<Void, ServiceUnit>> ServiceUnitLoadBalancer = defineModule(LoadBalancer.class, ServiceUnit.class.getSimpleName());
        private Modules() {
            super(ServerLifecycleManager.class);
        }
    }

    private final AcceptorLoop acceptorLoop;
    private final LoadBalancer<Void, HandshakeLoop> handshakeLoopLoadBalancer;
    private final LoadBalancer<Void, ServiceUnit> serviceUnitLoadBalancer;

    public ServerLifecycleManager(NexalithicBuilderContext context) {
        super(NexalithicServer.class.getSimpleName());
        this.acceptorLoop = context.getModule(MODULES.AcceptorLoop);
        this.handshakeLoopLoadBalancer = context.getModule(MODULES.HandshakeLoopLoadBalancer);
        this.serviceUnitLoadBalancer = context.getModule(MODULES.ServiceUnitLoadBalancer);
    }

    @Override
    public void onStart() {
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            serviceUnit.getStewardLoop().start();
            for (WorkerLoop workerLoop : serviceUnit.getWorkerLoops()) {
                workerLoop.start();
            }
        }
        for (HandshakeLoop handshakeLoop : handshakeLoopLoadBalancer.all()) {
            handshakeLoop.start();
        }
        acceptorLoop.start();
    }

    @Override
    public void onStop() {
        acceptorLoop.stop();
        for (HandshakeLoop handshakeLoop : handshakeLoopLoadBalancer.all()) {
            handshakeLoop.stop();
        }
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            serviceUnit.getStewardLoop().stop();
            for (WorkerLoop workerLoop : serviceUnit.getWorkerLoops()) {
                workerLoop.stop();
            }
        }
    }

    @Override
    public void onShutdown() {
        acceptorLoop.shutdown();
        for (HandshakeLoop handshakeLoop : handshakeLoopLoadBalancer.all()) {
            handshakeLoop.shutdown();
        }
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            serviceUnit.getStewardLoop().shutdown();
            for (WorkerLoop workerLoop : serviceUnit.getWorkerLoops()) {
                workerLoop.shutdown();
            }
        }
    }

    public AcceptorLoop getAcceptorLoop() {
        return acceptorLoop;
    }
}
