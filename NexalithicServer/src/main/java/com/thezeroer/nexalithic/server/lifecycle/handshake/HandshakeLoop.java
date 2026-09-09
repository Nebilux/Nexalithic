package com.thezeroer.nexalithic.server.lifecycle.handshake;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.module.ModulesDefinition;
import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.executor.BlockingTaskQueue;
import com.thezeroer.nexalithic.core.infra.executor.FixedTaskExecutor;
import com.thezeroer.nexalithic.core.infra.executor.TypedThreadFactory;
import com.thezeroer.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.thezeroer.nexalithic.core.infra.timer.TimeWheel;
import com.thezeroer.nexalithic.core.infra.timer.TimerContext;
import com.thezeroer.nexalithic.core.infra.timer.TimerCoordinator;
import com.thezeroer.nexalithic.core.io.loop.AbstractLoop;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.security.SecretKeyUtils;
import com.thezeroer.nexalithic.core.security.SecurityPolicy;
import com.thezeroer.nexalithic.core.session.SessionKey;
import com.thezeroer.nexalithic.server.NexalithicServer;
import com.thezeroer.nexalithic.server.lifecycle.ServerLifecycleManager;
import com.thezeroer.nexalithic.server.lifecycle.service.ServiceUnit;
import com.thezeroer.nexalithic.server.lifecycle.service.session.ServerSession;
import com.thezeroer.nexalithic.server.manager.SessionsManager;
import com.thezeroer.nexalithic.server.security.ServerSecurityPolicy;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 握手选择器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/06
 * @version 1.0.0
 */
public class HandshakeLoop extends AbstractLoop implements TimerCoordinator<PendingChannel> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, HandshakeLoop.class);
    public static final class Options extends AbstractLoop.Options {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {
            protected NexalithicOption<Integer> SlotCount() {
                return defineOptionLazy(context ->
                                Math.toIntExact(context.getOption(OPTIONS.MaxWaitTimeMillis) / context.getOption(OPTIONS.TimeWheel.TickMillis)) + 1
                        , OptionValidator.positive()
                );
            }
        };
        public final FixedTaskExecutor.Options FixedTaskExecutor = new FixedTaskExecutor.Options(holder) {
        };
        public final NexalithicOption<Integer> DispatchQueue_Capacity = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> DispatchQueue_DrainLimit = defineOption(
                256, OptionValidator.positive()
        );
        public final NexalithicOption<Long> MaxWaitTimeMillis = defineOption(
                3_000L, OptionValidator.positive()
        );
        public final NexalithicOption<Boolean> SharedFixedTaskExecutor = defineOption(
                true, OptionValidator.nonNull()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<TimeWheel<PendingChannel>> TimeWheel = defineModule(TimeWheel.class);
        public final NexalithicModule<FixedTaskExecutor<PendingChannel, ExecutorThread>> FixedTaskExecutor = defineModule(FixedTaskExecutor.class);
        private Modules() {
            super(HandshakeLoop.class);
        }
    }
    public record Constant(int DrainLimit) {}
    private static final Logger logger = LoggerFactory.getLogger(HandshakeLoop.class);
    private final Constant CONSTANT;
    private final SessionsManager sessionsManager;
    private final ServerSecurityPolicy securityPolicy;
    private final LoadBalancer<Void, ServiceUnit> serviceUnitLoadBalancer;
    private final TimeWheel<PendingChannel> timeWheel;
    private final FixedTaskExecutor<PendingChannel, ExecutorThread> executor;
    private final MpscArrayQueue<PendingChannel> dispatchQueue;
    private final SecureRandom secureRandom = new SecureRandom();

    public HandshakeLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        CONSTANT = context.getConstant(this.getClass(), Constant.class, () -> new Constant(
                context.getOption(OPTIONS.DispatchQueue_DrainLimit))
        );
        sessionsManager = context.getModule(NexalithicServer.MODULES.SessionsManager);
        securityPolicy = context.getModule(NexalithicServer.MODULES.SecurityPolicy);
        serviceUnitLoadBalancer = context.getModule(ServerLifecycleManager.MODULES.ServiceUnitLoadBalancer);
        timeWheel = context.getModule(MODULES.TimeWheel, () -> {
            TimeWheel<PendingChannel> timeWheel = new TimeWheel<>(
                    context.getOption(OPTIONS.TimeWheel.TickMillis),
                    context.getOption(OPTIONS.TimeWheel.SlotCount),
                    context.getOption(OPTIONS.TimeWheel.TickQuotaShift),
                    context.getOption(OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                    new GenericWrapperPool<>(
                            PoolStorageFactory.bounded(SpmcArrayQueue::new, context.getOption(OPTIONS.TimeWheel.WrapperPool_Capacity)),
                            PoolStrategyFactory.alwaysCreate(),
                            TimeWheel.ScheduleWrapper<PendingChannel>::new
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
        dispatchQueue = new MpscArrayQueue<>(context.getOption(OPTIONS.DispatchQueue_Capacity));
    }
    private FixedTaskExecutor<PendingChannel, ExecutorThread> createFixedTaskExecutor(NexalithicBuilderContext context, boolean shared) {
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
                (channel, executor) -> closeChannel(channel),
                (channel, thread) -> {
                    if (!channel.isActive()) {
                        return;
                    }
                    ByteBuffer writeBuffer = channel.getWriteBuffer();
                    switch (channel.getState()) {
                        case STEP_1 -> {
                            try {
                                KeyPair keyPair = SecretKeyUtils.generateKeyPair();
                                MessageDigest transcriptHash = SecretKeyUtils.createTranscriptHash();
                                byte[] rawPublickey = SecretKeyUtils.rawPublickey(keyPair.getPublic());
                                securityPolicy.certificatesToBuffer(writeBuffer);
                                securityPolicy.signature(rawPublickey, writeBuffer.put(rawPublickey));
                                transcriptHash.update(writeBuffer.flip());
                                writeBuffer.flip();
                                channel.setPrivateKey(keyPair.getPrivate()).setTranscriptHash(transcriptHash).getSelectionKey().interestOps(SelectionKey.OP_WRITE);
                                wakeupIfNeeded();
                            } catch (Exception e) {
                                closeChannel(channel);
                                logger.error(e.getMessage(), e);
                            }
                        }
                        case STEP_2 -> {
                            writeBuffer.clear();
                            ByteBuffer readBuffer = channel.getReadBuffer();
                            MessageDigest transcriptHash = channel.getTranscriptHash();
                            transcriptHash.update(readBuffer.flip().limit(SecretKeyUtils.ECDH_LENGTH));
                            try {
                                byte[] secret = SecretKeyUtils.compactSecret(channel.getPrivateKey(), readBuffer.rewind());
                                byte[] localFinished = SecretKeyUtils.generateFinished(secret, transcriptHash.digest());
                                SecretKeyContext signalingSecretKey = SecretKeyUtils.generateSessionSecretKey(secret, SecretKeyUtils.LABEL_SERVER_SIGNALING, SecretKeyUtils.LABEL_CLIENT_SIGNALING);
                                byte[] remoteFinished = signalingSecretKey.decrypt(readBuffer.limit(readBuffer.capacity()));
                                if (!MessageDigest.isEqual(localFinished, remoteFinished)) {
                                    closeChannel(channel);
                                    logger.warn("Finished verification failed");
                                    return;
                                }
                                writeBuffer.put(signalingSecretKey.encrypt(localFinished));
                                SessionKey.Immutable sessionKey = new SessionKey.Immutable(secureRandom.nextLong(), secureRandom.nextLong());
                                signalingSecretKey.encrypt(sessionKey.toByteBuffer(thread.getTempBuffer()).flip(), writeBuffer);
                                writeBuffer.flip();
                                channel.setSessionKey(sessionKey)
                                        .setSignalingSecretContext(signalingSecretKey)
                                        .setBusinessSecretContext(SecretKeyUtils.generateSessionSecretKey(secret, SecretKeyUtils.LABEL_SERVER_BUSINESS, SecretKeyUtils.LABEL_CLIENT_BUSINESS))
                                        .setState(PendingChannel.State.STEP_2);
                                channel.getSelectionKey().interestOps(SelectionKey.OP_WRITE);
                                wakeupIfNeeded();
                            } catch (BadPaddingException | IllegalBlockSizeException e) {
                                String remoteAddress = null;
                                try {
                                    remoteAddress = channel.getSocketChannel().getRemoteAddress().toString();
                                } catch (IOException ignored) {}
                                closeChannel(channel);
                                logger.warn("Security verification failed: [reason: {}] [remote: {}]", e.getMessage(), remoteAddress);
                            } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeySpecException e) {
                                closeChannel(channel);
                                logger.error("Cryptographic environment fatal error: ensure JCE provider (e.g., BouncyCastle) is correctly configured", e);
                            } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
                                closeChannel(channel);
                                logger.error("Invalid cryptographic parameters detected : {}", e.getMessage(), e);
                            } catch (ShortBufferException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
        );
    }

    public void dispatch(PendingChannel pendingChannel) {
        if (dispatchQueue.offer(pendingChannel)) {
            loadScore.increment();
            timeWheel.schedule(pendingChannel, pendingChannel.stamp(), HandshakeLoop.this);
            wakeupIfNeeded();
        } else {
            pendingChannel.closeChannel();
        }
    }

    @Override
    public boolean onAsyncEvent() {
        dispatchQueue.drain(channel -> {
            try {
                if (!channel.isActive()) {
                    return;
                }
                SelectionKey key = channel.getSocketChannel().configureBlocking(false).register(selector, SelectionKey.OP_READ);
                key.attach(channel.setSelectionKey(key));
                channel.getReadBuffer().limit(SecurityPolicy.MAGIC_NUMBER_LENGTH);
            } catch (IOException ignored) {}
        }, CONSTANT.DrainLimit);
        return dispatchQueue.isEmpty();
    }

    @Override
    public void onReadyEvent(SelectionKey key) {
        PendingChannel channel = (PendingChannel) key.attachment();
        channel.updateLastActiveTimeNanos(System.nanoTime());
        try {
            SocketChannel socketChannel = channel.getSocketChannel();
            if (key.isReadable()) {
                switch (channel.getState()) {
                    case STEP_1 -> {
                        if (verifyMagicNumber(channel)) {
                            if (channel.getType() == AbstractPacket.PacketType.SIGNALING) {
                                key.interestOps(0);
                                executor.submit(channel);
                            } else {
                                channel.setState(PendingChannel.State.STEP_2);
                            }
                        }
                    }
                    case STEP_2 -> {
                        ByteBuffer readBuffer = channel.getReadBuffer();
                        if (socketChannel.read(readBuffer) == -1) {
                            closeChannel(channel);
                        }
                        if (!readBuffer.hasRemaining()) {
                            if (channel.getType() == AbstractPacket.PacketType.SIGNALING) {
                                key.interestOps(0);
                                executor.submit(channel);
                            } else {
                                ServerSession session = sessionsManager.verifyAndConsumeToken(readBuffer, 0);
                                if (session != null) {
                                    key.cancel();
                                    loadScore.decrement();
                                    session.getServiceUnit().selectWorkerLoop().dispatch(channel.setSession(session));
                                } else {
                                    closeChannel(channel);
                                }
                            }
                        }
                    }
                }
            } else if (key.isWritable()) {
                ByteBuffer writeBuffer = channel.getWriteBuffer();
                socketChannel.write(writeBuffer);
                switch (channel.getState()) {
                    case STEP_1 -> {
                        if (!writeBuffer.hasRemaining()) {
                            channel.setState(PendingChannel.State.STEP_2);
                            key.interestOps(SelectionKey.OP_READ);
                        }
                    }
                    case STEP_2 -> {
                        if (!writeBuffer.hasRemaining()) {
                            key.cancel();
                            serviceUnitLoadBalancer.select(null).getStewardLoop().dispatch(channel);
                            loadScore.decrement();
                        }
                    }
                }
            }
        } catch (IOException e) {
            closeChannel(channel);
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

    @Override
    public long getExpiryTimeNanos(TimerContext<PendingChannel> context) {
        return context.target().getExpiryTimeNanos();
    }

    @Override
    public boolean isCancelled(TimerContext<PendingChannel> context) {
        return !context.target().isActive(context.targetStamp());
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<PendingChannel> context) {
        PendingChannel target = context.target();
        if (System.nanoTime() < target.getExpiryTimeNanos()) {
            return false;
        }
        logger.warn("[{}] handshake timeout", target.toString());
        closeChannel(target);
        return true;
    }

    private boolean verifyMagicNumber(PendingChannel channel) throws IOException {
        ByteBuffer readBuffer = channel.getReadBuffer();
        if (channel.getSocketChannel().read(readBuffer) == -1) {
            closeChannel(channel);
            return false;
        }
        if (!readBuffer.hasRemaining()) {
            if (readBuffer.flip().getLong() != SecurityPolicy.MAGIC_NUMBER) {
                closeChannel(channel);
                return false;
            } else {
                readBuffer.clear();
                if (channel.getType() != AbstractPacket.PacketType.SIGNALING) {
                    readBuffer.limit(SessionKey.LENGTH);
                }
                return true;
            }
        }
        return false;
    }
    private void closeChannel(PendingChannel channel) {
        if (channel.closeChannel()) {
            loadScore.decrement();
        }
    }

    public static class ExecutorThread extends Thread {
        private final ByteBuffer tempBuffer = ByteBuffer.allocate(SessionKey.LENGTH);

        private ExecutorThread(Runnable runnable, String name) {
            super(runnable, name);
        }

        public ByteBuffer getTempBuffer() {
            return tempBuffer.clear();
        }
    }
}
