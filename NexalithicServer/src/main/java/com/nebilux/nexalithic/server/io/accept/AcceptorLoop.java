package com.nebilux.nexalithic.server.io.accept;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.infra.concurrent.atomic.AtomicStateQueue;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.loop.SelectorLoop;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.handshake.HandshakeIngress;
import com.nebilux.nexalithic.server.routing.NetworkRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 接收器选择器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class AcceptorLoop extends SelectorLoop {
    public static final Options OPTIONS = Options.initOptions(Options.class, AcceptorLoop.class);
    public static final class Options extends SelectorLoop.Options {
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<HandshakeIngress> HandshakeIngress = defineModule(HandshakeIngress.class);
        Modules() {
            super(AcceptorLoop.class);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AcceptorLoop.class);
    private enum RegistrationState {
        OPEN,
        CLOSED
    }
    private record ListenerRegistration(
            NexalithicChannel.Kind kind,
            AdmissionStrategy<?> strategy,
            NetworkRouter.RouteRegistration routeRegistration
    ) {}
    private final AtomicStateQueue.StateHandle<RegistrationState> registrationState = new AtomicStateQueue.StateHandle<>(RegistrationState.OPEN);
    private final AtomicStateQueue<Runnable, RegistrationState> eventQueue = new AtomicStateQueue<>(
            new ConcurrentLinkedQueue<>(), registrationState, (state, event) -> state == RegistrationState.OPEN
    );
    private final HandshakeIngress handshakeIngress;
    private final NetworkRouter networkRouter;

    public AcceptorLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        handshakeIngress = context.getModule(MODULES.HandshakeIngress);
        networkRouter = context.getModule(NexalithicServer.MODULES.NetworkRouter);
        drainAsyncEventsCondition(() -> {
            eventQueue.drain(Runnable::run);
            if (registrationState.get() == RegistrationState.CLOSED) {
                for (SelectionKey key : registeredKeys().toArray(SelectionKey[]::new)) {
                    try {
                        closeListenerKey(key);
                    } catch (Exception failure) {
                        logger.error("Failed to close listener during shutdown", failure);
                    }
                }
            }
            return eventQueue.isEmpty();
        });
    }

    public boolean register(ServerSocketChannel channel, NexalithicChannel.Kind kind, AdmissionStrategy.Builder<?> strategyBuilder) {
        if (registrationState.get() != RegistrationState.OPEN) {
            closeRejectedListener(channel);
            return false;
        }
        if (!eventQueue.offer(() -> registerListener(channel, kind, strategyBuilder))) {
            closeRejectedListener(channel);
            return false;
        }
        wakeup();
        return true;
    }

    @Override
    protected void sealLoop() {
        registrationState.set(RegistrationState.CLOSED);
    }

    @Override
    protected void onSelectorKeyReady(SelectionKey key) {
        try {
            if (registrationState.get() == RegistrationState.CLOSED) {
                closeListenerKey(key);
                return;
            }
            if (!key.isAcceptable()) {
                return;
            }
            SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
            if (socketChannel == null) {
                return;
            }
            ListenerRegistration listenerRegistration = (ListenerRegistration) key.attachment();
            NexalithicChannel.Kind kind = listenerRegistration.kind();
            if (logger.isTraceEnabled()) {
                logger.trace("socket accepted [{}] [{}]", kind, socketChannel.getRemoteAddress());
            }
            listenerRegistration.strategy().submit(kind, socketChannel);
        } catch (Exception exception) {
            logger.error("Accept failed [{}]", key, exception);
        }
    }

    @Override
    protected void discardAsyncEvents() {
        eventQueue.drain(Runnable::run);
    }

    @Override
    protected void clearSelectionKey(SelectionKey key) throws Exception {
        closeListenerKey(key);
    }

    private void registerListener(ServerSocketChannel channel, NexalithicChannel.Kind kind, AdmissionStrategy.Builder<?> strategyBuilder) {
        SocketAddress address = null;
        SelectionKey selectionKey = null;
        NetworkRouter.RouteRegistration routeRegistration = null;
        try {
            address = channel.getLocalAddress();
            AdmissionStrategy<?> strategy = strategyBuilder.build(handshakeIngress);
            selectionKey = registerSelectableChannel(channel.configureBlocking(false), SelectionKey.OP_ACCEPT);
            int port = ((InetSocketAddress) address).getPort();
            routeRegistration = networkRouter.registerLocalEndpoint(kind, port);
            selectionKey.attach(new ListenerRegistration(kind, strategy, routeRegistration));
            logger.debug("Registered [{}] channel [{}] successfully. Strategy [{}]", kind, address, strategy.getName());
            loadScore.increment();
        } catch (Exception exception) {
            logger.error("Failed to register [{}] channel [{}]", kind, address, exception);
            if (selectionKey != null) {
                selectionKey.attach(null);
                selectionKey.cancel();
            }
            if (routeRegistration != null) {
                routeRegistration.close();
            }
            try {
                channel.close();
            } catch (IOException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
        }
    }

    private void closeListenerKey(SelectionKey key) throws IOException {
        Object attachment = key.attachment();
        key.attach(null);
        key.cancel();
        try {
            key.channel().close();
        } finally {
            if (attachment instanceof ListenerRegistration registration) {
                registration.routeRegistration().close();
                loadScore.decrement();
            }
        }
    }

    private void closeRejectedListener(ServerSocketChannel channel) {
        try {
            channel.close();
        } catch (IOException failure) {
            logger.debug("Failed to close listener rejected during shutdown", failure);
        }
    }
}
