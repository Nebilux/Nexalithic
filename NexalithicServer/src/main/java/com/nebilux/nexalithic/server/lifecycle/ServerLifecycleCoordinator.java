package com.nebilux.nexalithic.server.lifecycle;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.nebilux.nexalithic.core.lifecycle.LifecycleCoordinator;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.accept.AcceptorLoop;
import com.nebilux.nexalithic.server.io.handshake.HandshakeLoop;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.io.session.business.BusinessLoop;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * 服务端生命周期协调器。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ServerLifecycleCoordinator extends LifecycleCoordinator {
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

    public ServerLifecycleCoordinator(NexalithicBuilderContext context) {
        super(NexalithicServer.class.getSimpleName());
        this.acceptorLoop = context.getModule(MODULES.AcceptorLoop);
        this.handshakeLoopLoadBalancer = context.getModule(MODULES.HandshakeLoopLoadBalancer);
        this.serviceUnitLoadBalancer = context.getModule(MODULES.ServiceUnitLoadBalancer);
    }

    @Override
    protected CompletionStage<Void> onStart() {
        return startSessionLoops()
                .thenCompose(ignored -> startHandshakeLoops())
                .thenCompose(ignored -> startAcceptorLoop());
    }

    @Override
    protected CompletionStage<Void> onStop() {
        return awaitAll(List.of(
                invoke(this::stopAcceptorLoop),
                invoke(this::stopHandshakeLoops),
                invoke(this::stopSessionLoops)
        ));
    }

    @Override
    protected CompletionStage<Void> onShutdown() {
        CompletionStage<Void> termination = invoke(this::shutdownAcceptorLoop);
        termination = continueAfter(termination, this::shutdownHandshakeLoops);
        return continueAfter(termination, this::shutdownSessionLoops);
    }

    private CompletionStage<Void> startAcceptorLoop() {
        return acceptorLoop.start();
    }
    private CompletionStage<Void> stopAcceptorLoop() {
        return acceptorLoop.stop();
    }
    private CompletionStage<Void> shutdownAcceptorLoop() {
        return acceptorLoop.shutdown();
    }

    private CompletionStage<Void> startHandshakeLoops() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (HandshakeLoop loop : handshakeLoopLoadBalancer.all()) {
            stages.add(loop.start());
        }
        return awaitAll(stages);
    }
    private CompletionStage<Void> stopHandshakeLoops() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (HandshakeLoop loop : handshakeLoopLoadBalancer.all()) {
            stages.add(invoke(loop::stop));
        }
        return awaitAll(stages);
    }
    private CompletionStage<Void> shutdownHandshakeLoops() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (HandshakeLoop loop : handshakeLoopLoadBalancer.all()) {
            stages.add(invoke(loop::shutdown));
        }
        return awaitAll(stages);
    }

    private CompletionStage<Void> startSessionLoops() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            stages.add(serviceUnit.getSignalingLoop().start());
            for (BusinessLoop businessLoop : serviceUnit.getBusinessLoops()) {
                stages.add(businessLoop.start());
            }
        }
        return awaitAll(stages);
    }
    private CompletionStage<Void> stopSessionLoops() {
        List<CompletionStage<Void>> stages = new ArrayList<>();
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            stages.add(invoke(serviceUnit.getSignalingLoop()::stop));
            for (BusinessLoop businessLoop : serviceUnit.getBusinessLoops()) {
                stages.add(invoke(businessLoop::stop));
            }
        }
        return awaitAll(stages);
    }
    private CompletionStage<Void> shutdownSessionLoops() {
        List<CompletionStage<Void>> sessionStages = new ArrayList<>();
        for (ServiceUnit serviceUnit : serviceUnitLoadBalancer.all()) {
            sessionStages.add(invoke(serviceUnit.getSignalingLoop()::shutdown));
            for (BusinessLoop businessLoop : serviceUnit.getBusinessLoops()) {
                sessionStages.add(invoke(businessLoop::shutdown));
            }
        }
        return awaitAll(sessionStages);
    }

    private static CompletionStage<Void> awaitAll(List<CompletionStage<Void>> stages) {
        CompletableFuture<?>[] futures = stages.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures).handle((ignored, failure) -> {
            Throwable combined = null;
            for (CompletableFuture<?> future : futures) {
                try {
                    future.join();
                } catch (Throwable stageFailure) {
                    combined = mergeFailure(combined, unwrapFailure(stageFailure));
                }
            }
            if (combined != null) {
                throw new CompletionException(combined);
            }
            return null;
        });
    }

    private static CompletionStage<Void> continueAfter(CompletionStage<Void> previous, Supplier<CompletionStage<Void>> next) {
        return previous.handle((ignored, failure) -> unwrapFailure(failure))
                .thenCompose(previousFailure -> invoke(next).handle((ignored, nextFailure) -> {
                    Throwable combined = mergeFailure(previousFailure, unwrapFailure(nextFailure));
                    if (combined != null) {
                        throw new CompletionException(combined);
                    }
                    return null;
                }));
    }

    private static CompletionStage<Void> invoke(Supplier<CompletionStage<Void>> action) {
        try {
            CompletionStage<Void> stage = action.get();
            return stage == null
                    ? CompletableFuture.failedFuture(new NullPointerException("lifecycle completion stage"))
                    : stage;
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }
}
