package com.thezeroer.nexalithic.client.lifecycle;

import com.thezeroer.nexalithic.client.NexalithicClient;
import com.thezeroer.nexalithic.client.lifecycle.session.ClientSession;
import com.thezeroer.nexalithic.client.lifecycle.session.ClientSessionChannel;
import com.thezeroer.nexalithic.client.manager.LinkStatusManager;
import com.thezeroer.nexalithic.client.manager.NetworkRouter;
import com.thezeroer.nexalithic.client.messaging.ClientHandlerCoordinator;
import com.thezeroer.nexalithic.client.security.ClientSecurityPolicy;
import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.rate.DynamicRateController;
import com.thezeroer.nexalithic.core.io.loop.ChannelLoop;
import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.model.packet.signaling.BareSignal;
import com.thezeroer.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.thezeroer.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.security.SecretKeyUtils;
import com.thezeroer.nexalithic.core.security.SecurityPolicy;
import com.thezeroer.nexalithic.core.session.SessionKey;
import com.thezeroer.nexalithic.core.session.channel.NexalithicChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 通用选择器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/06
 * @version 1.0.0
 */
public class GeneralLoop extends ChannelLoop<ClientSessionChannel<?>> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, GeneralLoop.class);
    public static final class Options extends ChannelLoop.Options {
        public final DynamicRateController.Options DynamicRateController = new DynamicRateController.Options(holder) {};
        public final NexalithicOption<Long> HeartBeat_IntervalMillis = NexalithicOption.create(
                30_000L, OptionValidator.positive()
        );
        public Options(Class<?> holder) {
            super(holder);
        }
    }
    private record Constant(long HeartBeat_IntervalNanos, boolean DynamicRate_Enable, long DynamicRate_TickNanos) {}
    private static final Logger logger = LoggerFactory.getLogger(GeneralLoop.class);
    private final Constant CONSTANT;
    private final Queue<Runnable> eventQueue;
    private final LinkStatusManager linkStatusManager;
    private final ClientSecurityPolicy securityPolicy;
    private final ClientHandlerCoordinator handlerCoordinator;
    private final NetworkRouter networkRouter;
    private final DynamicRateController dynamicRateController;
    private final Function<Object[], ClientSession> sessionFactory;
    private long lastDynamicRateTickNanos;
    private volatile ClientSession session;

    public GeneralLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        CONSTANT = new Constant(
                TimeUnit.NANOSECONDS.convert(context.getOption(OPTIONS.HeartBeat_IntervalMillis), TimeUnit.MILLISECONDS),
                context.getOption(OPTIONS.DynamicRateController.Enable),
                TimeUnit.NANOSECONDS.convert(context.getOption(OPTIONS.DynamicRateController.TickMillis), TimeUnit.MILLISECONDS)
        );
        linkStatusManager = context.getModule(NexalithicClient.Modules.LinkStatusManager);
        securityPolicy = context.getModule(NexalithicClient.Modules.SecurityPolicy);
        handlerCoordinator = context.getModule(NexalithicClient.Modules.HandlerCoordinator);
        networkRouter = new NetworkRouter();
        eventQueue = new ConcurrentLinkedQueue<>();
        dynamicRateController = new DynamicRateController(
                context.getOption(OPTIONS.DynamicRateController.MinBps),
                context.getOption(OPTIONS.DynamicRateController.MaxBps),
                context.getOption(OPTIONS.DynamicRateController.InitialBps),
                context.getOption(OPTIONS.DynamicRateController.EwmaAlpha),
                context.getOption(OPTIONS.DynamicRateController.Headroom),
                context.getOption(OPTIONS.DynamicRateController.ChangeThreshold),
                context.getOption(OPTIONS.DynamicRateController.MinPublishIntervalMillis),
                context.getOption(OPTIONS.DynamicRateController.IncreaseStableTicks)
        );
        ClientSession.ClientChannelFactory channelFactory = new ClientSession.ClientChannelFactory(context, this);
        TaskScheduler taskScheduler = context.getModule(NexalithicClient.Modules.TaskScheduler);
        sessionFactory = objects -> new ClientSession(
                (SessionKey) objects[0],
                (SecretKeyContext) objects[1],
                (SecretKeyContext) objects[2],
                channelFactory,
                taskScheduler,
                networkRouter
        );
        lastDynamicRateTickNanos = System.nanoTime();
    }

    public boolean link(AbstractPacket.PacketType packetType, SocketChannel socketChannel, byte[] token) throws IOException,
            NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException,
            InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, ShortBufferException {
        socketChannel.write(ByteBuffer.allocate(SecurityPolicy.MAGIC_NUMBER_LENGTH).putLong(SecurityPolicy.MAGIC_NUMBER).flip());
        if (packetType == AbstractPacket.PacketType.SIGNALING) {
            MessageDigest transcriptHash = SecretKeyUtils.createTranscriptHash();
            int certificatesLength = securityPolicy.certificatesLength();
            int keyAndSignatureLength = SecretKeyUtils.ECDH_LENGTH + securityPolicy.signatureLength();
            ByteBuffer readBuffer = ByteBuffer.allocate(Math.max(certificatesLength + keyAndSignatureLength,
                    SecretKeyUtils.FINISHED_LENGTH + ClientSession.SESSION_KEY_LENGTH + SecretKeyContext.TAG_LENGTH * 2));
            if (socketChannel.read(readBuffer) == -1) {
                return false;
            }
            transcriptHash.update(readBuffer.flip());
            securityPolicy.certificatesFormBuffer(readBuffer.slice(0, certificatesLength));
            if (!securityPolicy.verify(readBuffer.slice(certificatesLength, keyAndSignatureLength))) {
                logger.warn("NexalithicCertificate verification failed");
                throw new SecurityException("NexalithicCertificate verification failed");
            }
            KeyPair keyPair = SecretKeyUtils.generateKeyPair();
            ByteBuffer writeBuffer = ByteBuffer.allocate(SecretKeyUtils.ECDH_LENGTH + SecretKeyUtils.FINISHED_LENGTH + SecretKeyContext.TAG_LENGTH);
            writeBuffer.put(SecretKeyUtils.rawPublickey(keyPair.getPublic()));
            transcriptHash.update(writeBuffer.flip());
            writeBuffer.limit(writeBuffer.capacity());
            byte[] secret = SecretKeyUtils.compactSecret(keyPair.getPrivate(), readBuffer.slice(certificatesLength, keyAndSignatureLength));
            byte[] localFinished = SecretKeyUtils.generateFinished(secret, transcriptHash.digest());
            SecretKeyContext signalingSecretKey = SecretKeyUtils.generateSessionSecretKey(secret, SecretKeyUtils.LABEL_CLIENT_SIGNALING, SecretKeyUtils.LABEL_SERVER_SIGNALING);
            writeBuffer.put((signalingSecretKey.encrypt(localFinished)));
            socketChannel.write(writeBuffer.flip());
            if (socketChannel.read(readBuffer.clear()) == -1) {
                return false;
            }
            byte[] remoteFinished = signalingSecretKey.decrypt(readBuffer.flip().limit(SecretKeyUtils.FINISHED_LENGTH + SecretKeyContext.TAG_LENGTH));
            if (!MessageDigest.isEqual(localFinished, remoteFinished)) {
                logger.warn("Finished verification failed");
                throw new SecurityException("Finished verification failed");
            }
            ByteBuffer tempBuffer = ByteBuffer.allocate(ClientSession.SESSION_KEY_LENGTH);
            signalingSecretKey.decrypt(readBuffer.position(readBuffer.limit()).limit(readBuffer.limit() + ClientSession.SESSION_KEY_LENGTH + SecretKeyContext.TAG_LENGTH), tempBuffer);
            session = sessionFactory.apply(new Object[]{
                    new SessionKey.Immutable(tempBuffer.flip(), 0),
                    signalingSecretKey,
                    SecretKeyUtils.generateSessionSecretKey(secret, SecretKeyUtils.LABEL_CLIENT_BUSINESS, SecretKeyUtils.LABEL_SERVER_BUSINESS)
            });
            logger.info("Link server succeeded");
        } else {
            socketChannel.write(ByteBuffer.wrap(token));
        }
        eventQueue.add(() -> {
            ClientSessionChannel<?> channel = (ClientSessionChannel<?>) session.getChannel(packetType);
            try {
                SelectionKey selectionKey = socketChannel.configureBlocking(false).register(selector, SelectionKey.OP_READ);
                selectionKey.attach(channel.updateChannel(selectionKey));
                logger.debug("[{}] channel updateSelectionKey succeeded", packetType);
                if (!channel.fragmenterIsEmpty() && channel.updateChannelInterest(SelectionKey.OP_WRITE, true)) {
                    channel.applyTargetInterest();
                }
                if (channel.getType() == AbstractPacket.PacketType.SIGNALING) {
                    linkStatusManager.trigger(LinkStatusManager.Status.LINKED);
                } else {
                    channel.resetDynamicRateState();
                }
            } catch (Exception e) {
                logger.error("[{}] channel updateSelectionKey failed", packetType, e);
                if (channel.getType() == AbstractPacket.PacketType.SIGNALING) {
                    linkStatusManager.trigger(LinkStatusManager.Status.UNLINKED, e instanceof IOException ? LinkStatusManager.Reason.NETWORK_ERROR : LinkStatusManager.Reason.PROTOCOL_ERROR, e);
                }
                closeChannel(channel);
            }
        });
        wakeupIfNeeded();
        return true;
    }

    public void unlink() {
        eventQueue.add(() -> {
            LinkStatusManager.Status current = linkStatusManager.getStatus();
            if (current == LinkStatusManager.Status.UNLINKED) {
                logger.info("Server already unlinked, skipping.");
                return;
            }
            logger.info("Initiating active unlink from state: {}", current);
            linkStatusManager.trigger(LinkStatusManager.Status.UNLINKED, LinkStatusManager.Reason.LOCAL_ACTIVE);
            if (session != null) {
                session.close();
                session = null;
                logger.info("Session closed and resources recycled.");
            }
            networkRouter.clear();
        });
        wakeupIfNeeded();
    }

    @Override
    protected boolean onAsyncEvent() {
        while (!eventQueue.isEmpty()) {
            eventQueue.poll().run();
        }
        if (session != null) {
            long now = System.nanoTime();
            if (now - session.getLastActiveTimeNanos() >= CONSTANT.HeartBeat_IntervalNanos) {
                session.pushSignalingPacket(BareSignal.HeartBeat);
                session.updateLastActiveTimeNanos(now);
            }
            if (CONSTANT.DynamicRate_Enable && now - lastDynamicRateTickNanos >= CONSTANT.DynamicRate_TickNanos) {
                long interval = now - lastDynamicRateTickNanos;
                lastDynamicRateTickNanos = now;
                ClientSessionChannel<?> businessChannel = session.getBusinessChannel();
                if (businessChannel.getState() == NexalithicChannel.State.Connected) {
                    long targetRate = businessChannel.evaluateDynamicRate(interval, now, dynamicRateController);
                    if (targetRate > 0) {
                        session.setRemoteBusinessChannelWriteRate(targetRate);
                    }
                }
            }
        } else {
            lastDynamicRateTickNanos = System.nanoTime();
        }
        return true;
    }

    @Override
    protected void onReadyEvent(SelectionKey selectionKey, ClientSessionChannel<?> channel) {
        try {
            if (selectionKey.isReadable()) {
                if (channel.read() == -1) {
                    closeChannel(channel, false);
                    return;
                }
                if (channel.getType() == AbstractPacket.PacketType.SIGNALING) {
                    while (channel.get() instanceof SignalingPacket packet) {
                        handleSignalPacket(packet);
                    }
                } else {
                    while (channel.get() instanceof BusinessPacket packet) {
                        handlerCoordinator.accept(session, packet);
                    }
                }
            } else if (selectionKey.isWritable()) {
                if (channel.write() == -1) {
                    selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE);
                }
            } else {
                closeChannel(channel, false);
            }
        } catch (IOException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Channel[{}] onReadyEvent[{}] error", channel.toString(), name, e);
            }
            closeChannel(channel, true);
        } catch (Exception e) {
            logger.warn("Channel[{}] onReadyEvent[{}] error", channel.toString(), name, e);
            closeChannel(channel, true);
        }
    }

    @Override
    protected void keyNotValid(SelectionKey selectionKey) {
        ClientSessionChannel<?> channel = (ClientSessionChannel<?>) selectionKey.attachment();
        if (channel.getType() == AbstractPacket.PacketType.SIGNALING) {
            closeChannel(channel, false);
        }
    }

    private void handleSignalPacket(SignalingPacket packet) throws Exception {
        switch (packet.getSignal()) {
            case SignalingPacket.Signal.BusinessChannelToken_Response -> {
                byte[] token = packet.getContent();
                Integer port = networkRouter.getPort(AbstractPacket.PacketType.BUSINESS);
                if (port == null) {
                    session.setBusinessChannelToken(token);
                } else {
                    link(AbstractPacket.PacketType.BUSINESS,
                            SocketChannel.open(new InetSocketAddress(networkRouter.getServerHost(), port)),
                            token);
                }
            }
            case SignalingPacket.Signal.BusinessChannelPort_Response -> {
                int port = ((ScalarSignal) packet).asInt();
                networkRouter.setPort(AbstractPacket.PacketType.BUSINESS, port);
                byte[] token = session.getBusinessChannelToken();
                if (token != null) {
                    link(AbstractPacket.PacketType.BUSINESS,
                            SocketChannel.open(new InetSocketAddress(networkRouter.getServerHost(), port)),
                            token);
                }
            }
            case SignalingPacket.Signal.BusinessChannelRate -> {
                long rate = ((ScalarSignal) packet).asLong();
                ClientSessionChannel<?> businessChannel = session.getBusinessChannel();
                businessChannel.updateWriteRate(rate);
                businessChannel.applyRate();
            }
        }
    }

    public ClientSession getSession() {
        return session;
    }
    public NetworkRouter getNetworkRouter() {
        return networkRouter;
    }

    private void closeChannel(ClientSessionChannel<?> channel, boolean reconnect) {
        String channelInfo = channel.toString();
        logger.debug("closeChannel[{}]", channelInfo);
        AbstractPacket.PacketType type = channel.getType();
        if (type == AbstractPacket.PacketType.SIGNALING) {
            channel.session().close();
            session = null;
        } else {
            channel.closeChannel();
        }
        if (!reconnect) {
            if (type == AbstractPacket.PacketType.SIGNALING) {
                logger.info("Signaling channel closed without reconnection request.");
                linkStatusManager.trigger(LinkStatusManager.Status.UNLINKED, LinkStatusManager.Reason.REMOTE_ACTIVE);
            } else {
                logger.debug("Business channel closed without reconnection request.");
            }
            return;
        }
        performReconnect(type);
    }
    private synchronized void performReconnect(AbstractPacket.PacketType type) {
        logger.info("Initiating reconnection sequence for channel type: {}", type);
        if (type == AbstractPacket.PacketType.SIGNALING) {
            linkStatusManager.trigger(LinkStatusManager.Status.RECONNECTING);
            boolean success = false;
            for (int i = 1; i < 6; i++) {
                logger.debug("Signaling reconnection attempt [{}/5] to {}", i, networkRouter.getServerAddress());
                try {
                    if (link(AbstractPacket.PacketType.SIGNALING, SocketChannel.open(networkRouter.getServerAddress()), null)) {
                        logger.info("Signaling reconnection successful at attempt {}", i);
                        success = true;
                        break;
                    }
                } catch (Exception e) {
                    logger.warn("Signaling reconnection attempt [{}/5] failed: {}", i, e.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Detailed error for attempt [{}]", i, e);
                    }
                }
                try {
                    Thread.sleep(3000 * i);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (!success) {
                logger.error("All 5 reconnection attempts failed for Signaling channel. Switching to UNLINKED.");
                linkStatusManager.trigger(LinkStatusManager.Status.UNLINKED, LinkStatusManager.Reason.NETWORK_ERROR);
            }
        } else {
            if (session == null) {
                logger.warn("Skip Business reconnection: No active session available.");
                return;
            }
            Integer port = networkRouter.getPort(AbstractPacket.PacketType.BUSINESS);
            if (port == null) {
                logger.info("Business port unknown, requesting BusinessChannelPort_Request via Signaling channel.");
                session.pushSignalingPacket(BareSignal.BusinessChannelPort_Request);
            } else {
                logger.debug("Retrieved existing business port from router: {}", port);
            }
            logger.info("Requesting new BusinessChannelToken via Signaling channel.");
            session.pushSignalingPacket(BareSignal.BusinessChannelToken_Request);
        }
    }
}
