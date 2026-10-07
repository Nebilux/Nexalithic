package com.nebilux.nexalithic.server.io.handshake;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.executor.BlockingTaskQueue;
import com.nebilux.nexalithic.core.infra.executor.FixedTaskExecutor;
import com.nebilux.nexalithic.core.infra.executor.TypedThreadFactory;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.nebilux.nexalithic.core.infra.recyclable.WrapperPool;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.infra.timer.TimerContext;
import com.nebilux.nexalithic.core.infra.timer.TimerCoordinator;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.loop.ChannelLoop;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.security.SecretKeyUtils;
import com.nebilux.nexalithic.core.security.SecurityPolicy;
import com.nebilux.nexalithic.core.session.SessionKey;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.lifecycle.ServerLifecycleCoordinator;
import com.nebilux.nexalithic.server.security.ServerSecurityPolicy;
import com.nebilux.nexalithic.server.session.ServerSession;
import com.nebilux.nexalithic.server.session.SessionRegistry;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 握手选择器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class HandshakeLoop extends ChannelLoop<HandshakeContext, HandshakeContext> implements HandshakeIngress, TimerCoordinator<HandshakeContext> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, HandshakeLoop.class);
    public static final class Options extends ChannelLoop.Options {
        public final GenericWrapperPool.Options WrapperPool = new GenericWrapperPool.Options(holder) {};
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {
            protected NexalithicOption<Integer> SlotCount() {
                return defineOptionLazy(context ->
                                Math.toIntExact(context.getOption(OPTIONS.MaxIdleTimeMillis) / context.getOption(OPTIONS.TimeWheel.TickMillis)) + 1
                        , OptionValidator.positive()
                );
            }
        };
        public final FixedTaskExecutor.Options FixedTaskExecutor = new FixedTaskExecutor.Options(holder) {};
        public final NexalithicOption<Boolean> SharedFixedTaskExecutor = defineOption(
                true, OptionValidator.nonNull()
        );
        public final NexalithicOption<Integer> AsyncQueue_ChunkSize = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> AsyncQueue_DrainLimit = defineOptionLazy(context ->
                Math.max(context.getOption(AsyncQueue_ChunkSize) / 4, 1), OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
        @Override
        protected long MaxIdleTimeMillis_Value() {
            return 3_000L;
        }
    }
    private static final Modules MODULES = new Modules();
    private static final class Modules extends ModulesDefinition {
        private final NexalithicModule<TimeWheel<HandshakeContext>> TimeWheel = defineModule(TimeWheel.class);
        private final NexalithicModule<FixedTaskExecutor<HandshakeContext, ExecutorThread>> FixedTaskExecutor = defineModule(FixedTaskExecutor.class);
        private Modules() {
            super(HandshakeLoop.class);
        }
    }

    protected record Constant(long MaxIdleTimeNanos, int AsyncQueue_DrainLimit) {}
    protected final Constant CONSTANT;
    private static final Logger logger = LoggerFactory.getLogger(HandshakeLoop.class);
    private final SessionRegistry sessionRegistry;
    private final ServerSecurityPolicy securityPolicy;
    private final WrapperPool<HandshakeContext> wrapperPool;
    private final LoadBalancer<Void, ServiceUnit> serviceUnitLoadBalancer;
    private final TimeWheel<HandshakeContext> timeWheel;
    private final FixedTaskExecutor<HandshakeContext, ExecutorThread> executor;
    private final MpscUnboundedArrayQueue<HandshakeContext> asyncQueue;
    private final SecureRandom secureRandom = new SecureRandom();

    public HandshakeLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        CONSTANT = context.getConstant(HandshakeLoop.class, Constant.class, () -> new Constant(
                TimeUnit.MILLISECONDS.toNanos(context.getOption(OPTIONS.MaxIdleTimeMillis)),
                context.getOption(OPTIONS.AsyncQueue_DrainLimit)
        ));
        sessionRegistry = context.getModule(NexalithicServer.MODULES.SessionRegistry);
        securityPolicy = context.getModule(NexalithicServer.MODULES.SecurityPolicy);
        serviceUnitLoadBalancer = context.getModule(ServerLifecycleCoordinator.MODULES.ServiceUnitLoadBalancer);
        timeWheel = context.getModule(MODULES.TimeWheel, () -> {
            TimeWheel<HandshakeContext> timeWheel = new TimeWheel<>(
                    context.getOption(OPTIONS.TimeWheel.TickMillis),
                    context.getOption(OPTIONS.TimeWheel.SlotCount),
                    context.getOption(OPTIONS.TimeWheel.TickQuotaShift),
                    context.getOption(OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                    new GenericWrapperPool<>(
                            PoolStorageFactory.bounded(SpmcArrayQueue::new, context.getOption(OPTIONS.TimeWheel.WrapperPool_Capacity)),
                            PoolStrategyFactory.alwaysCreate(),
                            TimeWheel.ScheduleWrapper<HandshakeContext>::new
                    ),
                    HandshakeLoop.class.getSimpleName()
            );
            timeWheel.start();
            return timeWheel;
        });
        if (context.getOption(OPTIONS.SharedFixedTaskExecutor)) {
            executor = context.getModule(MODULES.FixedTaskExecutor, () -> createFixedTaskExecutor(context, true));
        } else {
            executor = createFixedTaskExecutor(context,false);
        }

        int wrapperPool_StorageCapacity = context.getOption(OPTIONS.WrapperPool.StorageCapacity);
        int handshakeContext_ReadBufferCapacity = SecretKeyUtils.ECDH_LENGTH + SecretKeyUtils.FINISHED_LENGTH + SecretKeyContext.TAG_LENGTH;
        int handshakeContext_WriteBufferCapacity = Math.max(
                securityPolicy.certificatesLength() + SecretKeyUtils.ECDH_LENGTH + securityPolicy.signatureLength(),
                SecretKeyUtils.FINISHED_LENGTH + SessionKey.LENGTH + SecretKeyContext.TAG_LENGTH * 2
        );
        wrapperPool = new GenericWrapperPool<HandshakeContext, HandshakeContext>(
                PoolStorageFactory.bounded(MpmcArrayQueue::new, wrapperPool_StorageCapacity),
                PoolStrategyFactory.blocking(wrapperPool_StorageCapacity),
                owner -> new HandshakeContext(owner, this, handshakeContext_ReadBufferCapacity, handshakeContext_WriteBufferCapacity)
        ).warmUp(context.getOption(OPTIONS.WrapperPool.PrefillRatio));

        asyncQueue = new MpscUnboundedArrayQueue<>(context.getOption(OPTIONS.AsyncQueue_ChunkSize));

        drainAsyncEventsCondition(() -> {
            asyncQueue.drain(this::consumeAsyncCompletion, CONSTANT.AsyncQueue_DrainLimit());
            return asyncQueue.isEmpty();
        });
    }
    private FixedTaskExecutor<HandshakeContext, ExecutorThread> createFixedTaskExecutor(NexalithicBuilderContext context, boolean shared) {
        return new FixedTaskExecutor<>(
                context.getOption(OPTIONS.FixedTaskExecutor.CoreWorkerSize),
                context.getOption(OPTIONS.FixedTaskExecutor.MaxWorkerSize),
                context.getOption(OPTIONS.FixedTaskExecutor.KeepAliveTimeMillis),
                BlockingTaskQueue.of(shared
                        ? new MpmcArrayQueue<>(context.getOption(OPTIONS.FixedTaskExecutor.TaskQueue_Capacity))
                        : new SpmcArrayQueue<>(context.getOption(OPTIONS.FixedTaskExecutor.TaskQueue_Capacity))
                ),
                new TypedThreadFactory<>() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public ExecutorThread newThread(Runnable runnable) {
                        String name;
                        if (shared) {
                            name = "HandshakeLoop-FixedTaskExecutor-" + counter.getAndIncrement();
                        } else {
                            name = HandshakeLoop.this.name + "-FixedTaskExecutor-" + counter.getAndIncrement();
                        }
                        ExecutorThread thread = new ExecutorThread(runnable, name);
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                (handshakeContext, executor) -> handshakeContext.ownerLoop().submitDisconnectEvent(handshakeContext, Event.Disconnect.Reason.INTERNAL_FAILURE),
                this::executeAsyncTask
        );
    }

    @Override
    public boolean submit(NexalithicChannel.Kind kind, SocketChannel channel) {
        HandshakeContext context = wrapperPool.acquire();
        if (context == null) {
            return false;
        }
        try {
            context.init(kind, channel);
            if (!submitAcquireEvent(context)) {
                context.recycle();
            }
        } catch (Exception exception) {
            logger.error("Error while initializing HandshakeContext", exception);
            context.recycle();
        }
        return true;
    }

    @Override
    protected HandshakeContext onExecuteAcquireEvent(HandshakeContext handoff) {
        try {
            if (!handoff.isActive()) {
                return null;
            }
            handoff.attachSelectionKey(registerSelectableChannel(handoff.getSocketChannel().configureBlocking(false), SelectionKey.OP_READ));
            handoff.getReadBuffer().limit(SecurityPolicy.MAGIC_NUMBER_LENGTH);
            timeWheel.schedule(handoff, handoff.stamp(), this);
        } catch (IOException failure) {
            logger.debug("[{}] failed to register handshake channel", name, failure);
            return null;
        }
        return handoff;
    }

    @Override
    protected boolean onExecuteReleaseEvent(HandshakeContext channel) {
        SelectionKey key = channel.getSelectionKey();
        if (key != null) {
            key.cancel();
        }
        return true;
    }

    @Override
    protected void onExecuteDisconnectEvent(HandshakeContext channel, Event.Disconnect.Reason reason) {
        if (channel.getPhase().isAsync()) {
            channel.setPhase(HandshakeContext.Phase.RECYCLE_PENDING);
        } else {
            channel.recycle();
        }
    }

    @Override
    protected void onChannelReady(SelectionKey selectionKey, HandshakeContext context) {
        try {
            SocketChannel socketChannel = context.getSocketChannel();
            if (selectionKey.isReadable()) {
                handleReadable(selectionKey, context, socketChannel);
            } else if (selectionKey.isWritable()) {
                handleWritable(selectionKey, context, socketChannel);
            } else {
                submitDisconnectEvent(context, Event.Disconnect.Reason.PROTOCOL_VIOLATION);
            }
        } catch (IOException e) {
            submitDisconnectEvent(context, Event.Disconnect.Reason.IO_FAILURE);
        }
    }

    @Override
    protected void onSelectionKeyMigrated(SelectionKey oldKey, SelectionKey newKey) {
        if (oldKey.attachment() instanceof HandshakeContext context) {
            context.replaceSelectionKey(newKey);
        }
    }

    @Override
    public long getExpiryTimeNanos(TimerContext<HandshakeContext> context) {
        return context.target().getLastActiveTimeNanos() + CONSTANT.MaxIdleTimeNanos();
    }

    @Override
    public boolean isCancelled(TimerContext<HandshakeContext> context) {
        return !context.target().isActive(context.targetStamp());
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<HandshakeContext> context) {
        HandshakeContext target = context.target();
        if (System.nanoTime() < target.getLastActiveTimeNanos() + CONSTANT.MaxIdleTimeNanos()) {
            return false;
        }
        logger.warn("[{}] handshake timeout", target.toString());
        submitDisconnectEvent(target, Event.Disconnect.Reason.IDLE_TIMEOUT);
        return true;
    }

    private void handleReadable(SelectionKey key, HandshakeContext context, SocketChannel channel) throws IOException {
        ByteBuffer readBuffer = context.getReadBuffer();
        int read = channel.read(readBuffer);
        if (read == -1) {
            submitDisconnectEvent(context, Event.Disconnect.Reason.REMOTE_CLOSED);
            return;
        }
        if (read > 0) {
            context.updateLastActiveTimeNanos(System.nanoTime());
        }
        if (readBuffer.hasRemaining()) {
            return;
        }
        switch (context.getPhase()) {
            case READ_MAGIC -> {
                if (readBuffer.flip().getLong() != SecurityPolicy.MAGIC_NUMBER) {
                    submitDisconnectEvent(context, Event.Disconnect.Reason.PROTOCOL_VIOLATION);
                    return;
                }
                readBuffer.clear();
                if (context.getKind() == NexalithicChannel.Kind.Packet_Signaling) {
                    context.setPhase(HandshakeContext.Phase.PREPARE_SERVER_HELLO);
                    submitToExecutor(key, context);
                } else {
                    readBuffer.limit(SessionKey.LENGTH);
                    context.setPhase(HandshakeContext.Phase.READ_CHANNEL_TOKEN);
                }
            }
            case READ_CLIENT_HANDSHAKE -> {
                context.setPhase(HandshakeContext.Phase.VERIFY_CLIENT_HANDSHAKE);
                submitToExecutor(key, context);
            }
            case READ_CHANNEL_TOKEN -> {
                ServerSession session = sessionRegistry.verifyAndConsumeToken(readBuffer, 0, context.getKind());
                if (session == null) {
                    submitDisconnectEvent(context, Event.Disconnect.Reason.SECURITY_FAILURE);
                    return;
                }
                context.setPhase(HandshakeContext.Phase.READY);
                context.setTargetSession(session);
                if (!submitTransferEvent(context, context, session.getBusinessLoop())) {
                    submitDisconnectEvent(context, Event.Disconnect.Reason.TRANSFER_FAILURE);
                }
            }
        }
    }
    private void handleWritable(SelectionKey selectionKey, HandshakeContext context, SocketChannel channel) throws IOException {
        ByteBuffer writeBuffer = context.getWriteBuffer();
        int written = channel.write(writeBuffer);
        if (written > 0) {
            context.updateLastActiveTimeNanos(System.nanoTime());
        }
        if (writeBuffer.hasRemaining()) {
            return;
        }
        switch (context.getPhase()) {
            case WRITE_SERVER_HELLO -> {
                context.setPhase(HandshakeContext.Phase.READ_CLIENT_HANDSHAKE);
                selectionKey.interestOps(SelectionKey.OP_READ);
            }
            case WRITE_SERVER_FINISH -> {
                context.setPhase(HandshakeContext.Phase.READY);
                if (!submitTransferEvent(context, context, serviceUnitLoadBalancer.select(null).getSignalingLoop())) {
                    submitDisconnectEvent(context, Event.Disconnect.Reason.TRANSFER_FAILURE);
                }
            }
        }
    }
    private void submitToExecutor(SelectionKey key, HandshakeContext context) {
        key.interestOps(0);
        if (!executor.submit(context)) {
            context.setPhase(HandshakeContext.Phase.RECYCLE_PENDING);
            submitDisconnectEvent(context, Event.Disconnect.Reason.INTERNAL_FAILURE);
        }
    }

    private void executeAsyncTask(HandshakeContext context, ExecutorThread thread) {
        if (!context.isActive()) {
            return;
        }
        HandshakeContext.Phase phase = context.getPhase();
        Throwable failure = null;
        try {
            switch (phase) {
                case PREPARE_SERVER_HELLO -> prepareServerHello(context);
                case VERIFY_CLIENT_HANDSHAKE -> verifyClientHandshake(context, thread);
                case RECYCLE_PENDING -> {}
                default -> throw new IllegalStateException("Unexpected asynchronous handshake phase: " + phase);
            }
        } catch (Throwable throwable) {
            failure = throwable;
        } finally {
            context.ownerLoop().publishAsyncCompletion(context, failure);
        }
    }
    private void prepareServerHello(HandshakeContext context) throws Exception {
        ByteBuffer writeBuffer = context.getWriteBuffer();
        KeyPair keyPair = SecretKeyUtils.generateKeyPair();
        context.setPrivateKey(keyPair.getPrivate());
        byte[] rawPublickey = SecretKeyUtils.rawPublickey(keyPair.getPublic());
        securityPolicy.certificatesToBuffer(writeBuffer);
        securityPolicy.signature(rawPublickey, writeBuffer.put(rawPublickey));
        writeBuffer.flip();
    }
    private void verifyClientHandshake(HandshakeContext context, ExecutorThread thread) throws Exception {
        ByteBuffer readBuffer = context.getReadBuffer();
        ByteBuffer writeBuffer = context.getWriteBuffer();
        MessageDigest messageDigest = SecretKeyUtils.createTranscriptHash();
        messageDigest.update(writeBuffer.rewind());
        messageDigest.update(readBuffer.flip().limit(SecretKeyUtils.ECDH_LENGTH));
        byte[] secret = SecretKeyUtils.compactSecret(context.getPrivateKey(), readBuffer.rewind());
        byte[] localFinished = SecretKeyUtils.generateFinished(secret, messageDigest.digest());
        SecretKeyContext signalingSecretContext = SecretKeyUtils.generateSessionSecretKey(secret, SecretKeyUtils.LABEL_SERVER_SIGNALING, SecretKeyUtils.LABEL_CLIENT_SIGNALING);
        byte[] remoteFinished = signalingSecretContext.decrypt(readBuffer.limit(readBuffer.capacity()));
        if (!MessageDigest.isEqual(localFinished, remoteFinished)) {
            throw new SecurityException("Finished verification failed");
        }
        SecretKeyContext businessSecretContext = SecretKeyUtils.generateSessionSecretKey(secret, SecretKeyUtils.LABEL_SERVER_BUSINESS, SecretKeyUtils.LABEL_CLIENT_BUSINESS);
        SessionKey.Immutable sessionKey = new SessionKey.Immutable(secureRandom.nextLong(), secureRandom.nextLong());
        signalingSecretContext.encrypt(sessionKey.toByteBuffer(thread.getTempBuffer()).flip(), writeBuffer.clear().put(signalingSecretContext.encrypt(localFinished)));
        context.setSessionMaterial(sessionKey, signalingSecretContext, businessSecretContext);
        writeBuffer.flip();
    }
    private void publishAsyncCompletion(HandshakeContext context, Throwable failure) {
        if (failure != null) {
            context.setAsyncFailure(failure);
        }
        asyncQueue.offer(context);
        wakeup();
    }
    private void consumeAsyncCompletion(HandshakeContext context) {
        HandshakeContext.Phase phase = context.getPhase();
        if (phase == HandshakeContext.Phase.RECYCLE_PENDING) {
            context.recycle();
            return;
        }
        Throwable failure = context.takeAsyncFailure();
        if (failure != null) {
            logger.warn("Handshake async operation failed", failure);
            context.setPhase(HandshakeContext.Phase.RECYCLE_PENDING);
            submitDisconnectEvent(context, failure instanceof SecurityException ? Event.Disconnect.Reason.SECURITY_FAILURE : Event.Disconnect.Reason.INTERNAL_FAILURE);
            return;
        }
        try {
            switch (phase) {
                case PREPARE_SERVER_HELLO -> context.setPhase(HandshakeContext.Phase.WRITE_SERVER_HELLO);
                case VERIFY_CLIENT_HANDSHAKE -> context.setPhase(HandshakeContext.Phase.WRITE_SERVER_FINISH);
                default -> throw new IllegalStateException("Unexpected asynchronous handshake phase: " + phase);
            }
            context.getSelectionKey().interestOps(SelectionKey.OP_WRITE);
        } catch (Exception exception) {
            logger.error("[{}] handshake phase failed", context.getPhase(), exception);
            submitDisconnectEvent(context, Event.Disconnect.Reason.INTERNAL_FAILURE);
        }
    }

    private static class ExecutorThread extends Thread {
        private final ByteBuffer tempBuffer = ByteBuffer.allocate(SessionKey.LENGTH);
        private ExecutorThread(Runnable runnable, String name) {
            super(runnable, name);
        }
        public ByteBuffer getTempBuffer() {
            return tempBuffer.clear();
        }
    }
}
