package com.nebilux.nexalithic.server.lifecycle;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.nebilux.nexalithic.core.lifecycle.LifecycleManager;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.accept.AcceptorLoop;
import com.nebilux.nexalithic.server.io.handshake.HandshakeLoop;
import com.nebilux.nexalithic.server.io.session.business.BusinessLoop;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;

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
            serviceUnit.getSignalingLoop().start();
            for (BusinessLoop businessLoop : serviceUnit.getBusinessLoops()) {
                businessLoop.start();
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
            serviceUnit.getSignalingLoop().stop();
            for (BusinessLoop businessLoop : serviceUnit.getBusinessLoops()) {
                businessLoop.stop();
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
            serviceUnit.getSignalingLoop().shutdown();
            for (BusinessLoop businessLoop : serviceUnit.getBusinessLoops()) {
                businessLoop.shutdown();
            }
        }
    }

    public AcceptorLoop getAcceptorLoop() {
        return acceptorLoop;
    }
}
