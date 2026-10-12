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

package com.nebilux.nexalithic.server.lifecycle;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.nebilux.nexalithic.core.lifecycle.AbstractLifecycle;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.accept.AcceptorLoop;
import com.nebilux.nexalithic.server.io.handshake.HandshakeLoop;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * 服务端生命周期协调器。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ServerLifecycleCoordinator extends AbstractLifecycle {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, ServerLifecycleCoordinator.class);
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
            super(ServerLifecycleCoordinator.class);
        }
    }

    private final AcceptorLoop acceptorLoop;
    private final LoadBalancer<Void, HandshakeLoop> handshakeLoopLoadBalancer;
    private final LoadBalancer<Void, ServiceUnit> serviceUnitLoadBalancer;
    private final TaskScheduler taskScheduler;

    public ServerLifecycleCoordinator(NexalithicBuilderContext context) {
        super(NexalithicServer.class.getSimpleName());
        this.acceptorLoop = context.getModule(MODULES.AcceptorLoop);
        this.handshakeLoopLoadBalancer = context.getModule(MODULES.HandshakeLoopLoadBalancer);
        this.serviceUnitLoadBalancer = context.getModule(MODULES.ServiceUnitLoadBalancer);
        this.taskScheduler = context.getModule(NexalithicServer.MODULES.TaskScheduler);
    }

    @Override
    protected boolean logMilestonesAtInfo() {
        return true;
    }

    @Override
    protected CompletionStage<Void> doStart() {
        return taskScheduler.start()
                .thenCompose(ignored -> startServiceUnits())
                .thenCompose(ignored -> startHandshakeLoops())
                .thenCompose(ignored -> startAcceptorLoop());
    }

    @Override
    protected CompletionStage<Void> doShutdown(ShutdownMode mode) {
        return mode == ShutdownMode.DRAIN ? drainComponents() : abortComponents();
    }

    private CompletionStage<Void> drainComponents() {
        CompletionStage<Void> termination = invoke(this::drainAcceptorLoop);
        termination = continueAfter(termination, this::drainHandshakeLoops);
        termination = continueAfter(termination, this::drainServiceUnits);
        return continueAfter(termination, () -> taskScheduler.shutdown(ShutdownMode.ABORT));
    }

    private CompletionStage<Void> abortComponents() {
        CompletionStage<Void> termination = awaitAll(List.of(
                invoke(this::abortAcceptorLoop),
                invoke(this::abortHandshakeLoops),
                invoke(this::abortServiceUnits)
        ));
        return continueAfter(termination, () -> taskScheduler.shutdown(ShutdownMode.ABORT));
    }

    private CompletionStage<Void> startAcceptorLoop() {
        return acceptorLoop.start();
    }
    private CompletionStage<Void> drainAcceptorLoop() {
        return acceptorLoop.shutdown(ShutdownMode.DRAIN);
    }
    private CompletionStage<Void> abortAcceptorLoop() {
        return acceptorLoop.shutdown(ShutdownMode.ABORT);
    }

    private CompletionStage<Void> startHandshakeLoops() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (HandshakeLoop loop : handshakeLoopLoadBalancer.all()) {
            stages.add(loop.start());
        }
        return awaitAll(stages);
    }
    private CompletionStage<Void> drainHandshakeLoops() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (HandshakeLoop loop : handshakeLoopLoadBalancer.all()) {
            stages.add(invoke(() -> loop.shutdown(ShutdownMode.DRAIN)));
        }
        return awaitAll(stages);
    }
    private CompletionStage<Void> abortHandshakeLoops() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (HandshakeLoop loop : handshakeLoopLoadBalancer.all()) {
            stages.add(invoke(() -> loop.shutdown(ShutdownMode.ABORT)));
        }
        return awaitAll(stages);
    }

    private CompletionStage<Void> startServiceUnits() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            stages.add(invoke(serviceUnit::start));
        }
        return awaitAll(stages);
    }
    private CompletionStage<Void> drainServiceUnits() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            stages.add(invoke(() -> serviceUnit.shutdown(ShutdownMode.DRAIN)));
        }
        return awaitAll(stages);
    }
    private CompletionStage<Void> abortServiceUnits() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            stages.add(invoke(() -> serviceUnit.shutdown(ShutdownMode.ABORT)));
        }
        return awaitAll(stages);
    }
}
