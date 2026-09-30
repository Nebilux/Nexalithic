package com.nebilux.nexalithic.server.io.accept;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.loop.SelectorLoop;
import com.nebilux.nexalithic.server.io.handshake.HandshakeIngress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 接收器选择器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/06
 * @version 1.0.0
 */
public class AcceptorLoop extends SelectorLoop {
    public static final Options OPTIONS = Options.initOptions(Options.class, AcceptorLoop.class);
    public static final class Options extends SelectorLoop.Options {
        public final NexalithicOption<Integer> FiltrationContextPool_Capacity = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> FiltrationContextPool_Limit = defineOption(
                FiltrationContextPool_Capacity.defaultValue() * 2, OptionValidator.positive()
        );
        public final NexalithicOption<Double> FiltrationContextPool_PrefillRatio = defineOption(
                0.5, OptionValidator.unitInterval()
        );
        public final NexalithicOption<Integer> PendingChannelPool_Capacity = defineOption(
                4096, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> PendingChannelPool_Limit = defineOption(
                PendingChannelPool_Capacity.defaultValue() * 2, OptionValidator.positive()
        );
        public final NexalithicOption<Double> PendingChannelPool_PrefillRatio = defineOption(
                0.5, OptionValidator.unitInterval()
        );
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
    private record ListenerRegistration(NexalithicChannel.Kind kind, AdmissionStrategy<?> strategy) {}
    private final Queue<Runnable> eventQueue = new ConcurrentLinkedQueue<>();
    private final HandshakeIngress handshakeIngress;

    public AcceptorLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        handshakeIngress = context.getModule(MODULES.HandshakeIngress);
        drainAsyncEventsCondition(() -> {
            while (!eventQueue.isEmpty()) {
                eventQueue.poll().run();
            }
            return true;
        });
    }

    public void register(ServerSocketChannel channel, NexalithicChannel.Kind kind, AdmissionStrategy.Builder<?> strategyBuilder) {
        eventQueue.add(() -> {
            SocketAddress address = null;
            try {
                address = channel.getLocalAddress();
                AdmissionStrategy<?> strategy = strategyBuilder.build(handshakeIngress);
                registerSelectableChannel(channel.configureBlocking(false), SelectionKey.OP_ACCEPT).attach(new ListenerRegistration(kind, strategy));
                logger.debug("Registered [{}] channel [{}] successfully. Strategy [{}]", kind, address, strategy.getName());
                loadScore.increment();
            } catch (Exception exception) {
                logger.error("Failed to register [{}] channel [{}]", kind, address, exception);
            }
        });
        wakeup();
    }

    @Override
    protected void onSelectorKeyReady(SelectionKey key) {
        try {
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
    protected void onSelectionKeyMigrated(SelectionKey oldKey, SelectionKey newKey) {}

    @Override
    protected void clearSelectionKey(SelectionKey key) throws Exception {
        key.channel().close();
    }
}
