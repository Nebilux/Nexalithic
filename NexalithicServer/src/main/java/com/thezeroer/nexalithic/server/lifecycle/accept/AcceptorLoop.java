package com.thezeroer.nexalithic.server.lifecycle.accept;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.WrapperPool;
import com.thezeroer.nexalithic.core.io.loop.AbstractLoop;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.security.SecretKeyUtils;
import com.thezeroer.nexalithic.server.NexalithicServer;
import com.thezeroer.nexalithic.server.lifecycle.ServerLifecycleManager;
import com.thezeroer.nexalithic.server.lifecycle.accept.filter.FiltrationContext;
import com.thezeroer.nexalithic.server.lifecycle.handshake.HandshakeLoop;
import com.thezeroer.nexalithic.server.lifecycle.handshake.PendingChannel;
import com.thezeroer.nexalithic.server.lifecycle.service.session.ServerSession;
import com.thezeroer.nexalithic.server.security.ServerSecurityPolicy;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * 接收器选择器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/06
 * @version 1.0.0
 */
public class AcceptorLoop extends AbstractLoop {
    public static final Options OPTIONS = Options.initOptions(Options.class, AcceptorLoop.class);
    public static final class Options extends AbstractLoop.Options {
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
    private static final Logger logger = LoggerFactory.getLogger(AcceptorLoop.class);
    private final Queue<Runnable> eventQueue = new ConcurrentLinkedQueue<>();
    private final WrapperPool<FiltrationContext> filtrationContextPool;
    private final WrapperPool<PendingChannel> pendingChannelPool;
    private final LoadBalancer<Void, HandshakeLoop> handshakeLoopBalancer;

    public AcceptorLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        ServerSecurityPolicy securityPolicy = context.getModule(NexalithicServer.MODULES.SecurityPolicy);
        PendingChannel.Constant pendingChannelConstant = new PendingChannel.Constant(
                TimeUnit.NANOSECONDS.convert(context.getOption(HandshakeLoop.OPTIONS.MaxWaitTimeMillis), TimeUnit.MILLISECONDS),
                SecretKeyUtils.ECDH_LENGTH + SecretKeyUtils.FINISHED_LENGTH + SecretKeyContext.TAG_LENGTH,
                Math.max(securityPolicy.certificatesLength() + SecretKeyUtils.ECDH_LENGTH + securityPolicy.signatureLength(),
                        SecretKeyUtils.FINISHED_LENGTH + ServerSession.SESSION_KEY_LENGTH + SecretKeyContext.TAG_LENGTH * 2)
        );
        handshakeLoopBalancer = context.getModule(ServerLifecycleManager.MODULES.HandshakeLoopLoadBalancer);
        pendingChannelPool = new GenericWrapperPool<PendingChannel, PendingChannel>(
                PoolStorageFactory.bounded(MpscArrayQueue::new, context.getOption(OPTIONS.PendingChannelPool_Capacity)),
                PoolStrategyFactory.blocking(context.getOption(OPTIONS.PendingChannelPool_Limit)),
                owner -> new PendingChannel(owner, pendingChannelConstant)
        ).warmUp(context.getOption(OPTIONS.PendingChannelPool_PrefillRatio));
        filtrationContextPool = new GenericWrapperPool<FiltrationContext, FiltrationContext>(
                PoolStorageFactory.bounded(MpscArrayQueue::new, context.getOption(OPTIONS.FiltrationContextPool_Capacity)),
                PoolStrategyFactory.blocking(context.getOption(OPTIONS.FiltrationContextPool_Limit)),
                owner -> new FiltrationContext(owner, handshakeLoopBalancer, pendingChannelPool)
        ).warmUp(context.getOption(OPTIONS.FiltrationContextPool_PrefillRatio));
    }

    public void dispatch(AbstractPacket.PacketType packetType, ServerSocketChannel serverSocketChannel, FiltrationStrategy strategy) {
        eventQueue.add(() -> {
            SocketAddress address = null;
            try {
                address = serverSocketChannel.getLocalAddress();
                serverSocketChannel.configureBlocking(false).register(selector, SelectionKey.OP_ACCEPT, packetType).attach(strategy.setType(packetType));
                if (logger.isDebugEnabled()) {
                    logger.debug("Registered [{}] channel [{}] successfully. Strategy [{}]", packetType, address, strategy.getName());
                }
                loadScore.increment();
            } catch (Exception e) {
                logger.error("Failed to register [{}] channel [{}]", packetType, address, e);
            }
        });
        wakeupIfNeeded();
    }

    @Override
    protected boolean onAsyncEvent() {
        while (!eventQueue.isEmpty()) {
            eventQueue.poll().run();
        }
        return true;
    }

    @Override
    protected void onReadyEvent(SelectionKey selectionKey) throws IOException {
        if (!selectionKey.isAcceptable()) {
            return;
        }
        SocketChannel socketChannel = ((ServerSocketChannel) selectionKey.channel()).accept();
        if (socketChannel == null) {
            return;
        }
        FiltrationStrategy filtrationStrategy = (FiltrationStrategy) selectionKey.attachment();
        if (logger.isTraceEnabled()) {
            logger.trace("socket accepted [{}] [{}]", filtrationStrategy.getType(), socketChannel.getRemoteAddress());
        }
        if (filtrationStrategy.enable()) {
            filtrationStrategy.handle(socketChannel, filtrationContextPool.acquire().init(filtrationStrategy.getType(), socketChannel).unwrap());
        } else {
            handshakeLoopBalancer.select(null).dispatch(pendingChannelPool.acquire().init(filtrationStrategy.getType(), socketChannel).unwrap());
        }
    }

    @Override
    protected void onShuttingDown() {
        for (SelectionKey key : selector.keys()) {
            try {
                key.channel().close();
            } catch (IOException ignored) {}
        }
    }
}
