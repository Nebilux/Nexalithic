package com.nebilux.nexalithic.client.io.session;

import com.nebilux.nexalithic.client.NexalithicClient;
import com.nebilux.nexalithic.client.manager.LinkStatusManager;
import com.nebilux.nexalithic.client.manager.NetworkRouter;
import com.nebilux.nexalithic.client.messaging.ClientHandlerCoordinator;
import com.nebilux.nexalithic.client.security.ClientSecurityPolicy;
import com.nebilux.nexalithic.client.session.ClientChannelFactory;
import com.nebilux.nexalithic.client.session.ClientSession;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.rate.DynamicRateController;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.loop.ChannelLoop;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.BareSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.security.SecretKeyUtils;
import com.nebilux.nexalithic.core.security.SecurityPolicy;
import com.nebilux.nexalithic.core.session.SessionChannel;
import com.nebilux.nexalithic.core.session.SessionKey;
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
 * 客户端会话循环
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/06
 * @version 1.0.0
 */
public class ClientSessionLoop extends SessionLoop<ChannelLoop.Handoff, SessionChannel<?, ClientSession>> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, ClientSessionLoop.class);
    public static final class Options extends SessionLoop.Options {
        public final DynamicRateController.Options DynamicRateController = new DynamicRateController.Options(holder) {};
        private Options(Class<?> holder) {
            super(holder);
        }
        @Override
        protected long MaxIdleTimeMillis_Value() {
            return 30_000L;
        }
    }
    private record Constant(long HeartBeat_IntervalNanos, boolean DynamicRate_Enable, long DynamicRate_TickNanos) {}
    private static final Logger logger = LoggerFactory.getLogger(ClientSessionLoop.class);
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

    public ClientSessionLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        CONSTANT = new Constant(
                TimeUnit.NANOSECONDS.convert(context.getOption(OPTIONS.MaxIdleTimeMillis), TimeUnit.MILLISECONDS),
                context.getOption(OPTIONS.DynamicRateController.Enable),
                TimeUnit.NANOSECONDS.convert(context.getOption(OPTIONS.DynamicRateController.TickMillis), TimeUnit.MILLISECONDS)
        );
        linkStatusManager = context.getModule(NexalithicClient.MODULES.LinkStatusManager);
        securityPolicy = context.getModule(NexalithicClient.MODULES.SecurityPolicy);
        handlerCoordinator = context.getModule(NexalithicClient.MODULES.HandlerCoordinator);
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
        ClientChannelFactory channelFactory = new ClientChannelFactory(context, this);
        TaskScheduler taskScheduler = context.getModule(NexalithicClient.MODULES.TaskScheduler);
        sessionFactory = objects -> new ClientSession(
                (SessionKey) objects[0],
                (SecretKeyContext) objects[1],
                (SecretKeyContext) objects[2],
                channelFactory,
                taskScheduler,
                networkRouter
        );
        lastDynamicRateTickNanos = System.nanoTime();
        isDrainCondition(eventQueue::isEmpty);
        drainAsyncEventsCondition(() -> {
            while (!eventQueue.isEmpty()) {
                eventQueue.poll().run();
            }
            if (session != null) {
                long now = System.nanoTime();
                if (now - session.getLastActiveTimeNanos() >= CONSTANT.HeartBeat_IntervalNanos) {
                    session.pushSignalingPacket(BareSignal.HeartBeat);
                }
                if (CONSTANT.DynamicRate_Enable && now - lastDynamicRateTickNanos >= CONSTANT.DynamicRate_TickNanos) {
                    long interval = now - lastDynamicRateTickNanos;
                    lastDynamicRateTickNanos = now;
                    SessionChannel<?, ClientSession> businessChannel = session.getBusinessChannel();
                    if (businessChannel.getState() == NexalithicChannel.State.Opened) {
                        long targetRate = businessChannel.evaluateDynamicRate(interval, now, dynamicRateController);
                        if (targetRate > 0) {
                            session.setRemoteBusinessChannelWriteRate(targetRate);
                        }
                    }
                }
            } else {
                lastDynamicRateTickNanos = System.nanoTime();
            }
            return eventQueue.isEmpty();
        });
    }

    public boolean link(AbstractPacket.PacketType packetType, SocketChannel socketChannel, byte[] token) throws IOException,
            NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException,
            InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, ShortBufferException {
        socketChannel.write(ByteBuffer.allocate(SecurityPolicy.MAGIC_NUMBER_LENGTH).putLong(SecurityPolicy.MAGIC_NUMBER).flip());
        if (packetType == AbstractPacket.PacketType.Signaling) {
            MessageDigest transcriptHash = SecretKeyUtils.createTranscriptHash();
            int certificatesLength = securityPolicy.certificatesLength();
            int keyAndSignatureLength = SecretKeyUtils.ECDH_LENGTH + securityPolicy.signatureLength();
            ByteBuffer readBuffer = ByteBuffer.allocate(Math.max(certificatesLength + keyAndSignatureLength,
                    SecretKeyUtils.FINISHED_LENGTH + SessionKey.LENGTH + SecretKeyContext.TAG_LENGTH * 2));
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
            ByteBuffer tempBuffer = ByteBuffer.allocate(SessionKey.LENGTH);
            signalingSecretKey.decrypt(readBuffer.position(readBuffer.limit()).limit(readBuffer.limit() + SessionKey.LENGTH + SecretKeyContext.TAG_LENGTH), tempBuffer);
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
            SessionChannel<?, ClientSession> channel = session.getChannel(packetType);
            try {
                SelectionKey selectionKey = registerSelectableChannel(socketChannel.configureBlocking(false), SelectionKey.OP_READ);
                channel.open(socketChannel, selectionKey, (InetSocketAddress) socketChannel.getRemoteAddress());
                logger.debug("[{}] channel open succeeded", packetType);
                if (!channel.fragmenterIsEmpty()) {
                    channel.updateInterest(SelectionKey.OP_WRITE, true);
                }
                if (channel.getChannelType() == AbstractPacket.PacketType.Signaling) {
                    linkStatusManager.trigger(LinkStatusManager.Status.LINKED);
                } else {
                    channel.resetDynamicRateState();
                }
            } catch (Exception e) {
                logger.error("[{}] channel open failed", packetType, e);
                if (channel.getChannelType() == AbstractPacket.PacketType.Signaling) {
                    linkStatusManager.trigger(LinkStatusManager.Status.UNLINKED, e instanceof IOException ? LinkStatusManager.Reason.NETWORK_ERROR : LinkStatusManager.Reason.PROTOCOL_ERROR, e);
                }
                submitDisconnectEvent(channel, Event.Disconnect.Reason.IO_FAILURE);
            }
        });
        wakeup();
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
        wakeup();
    }

    @Override
    protected boolean onExecuteAcquireEvent(Handoff handoff) {
        return true;
    }

    @Override
    protected boolean onExecuteReleaseEvent(SessionChannel<?, ClientSession> channel) {
        return true;
    }

    @Override
    protected boolean onExecuteDisconnectEvent(SessionChannel<?, ClientSession> channel, Event.Disconnect.Reason reason) {
        return true;
    }

    @Override
    protected void onChannelReady(SelectionKey selectionKey, SessionChannel<?, ClientSession> channel) throws IOException {
        try {
            if (selectionKey.isReadable()) {
                if (channel.read() == -1) {
                    closeChannel(channel, false);
                    return;
                }
                if (channel.getChannelType() == AbstractPacket.PacketType.Signaling) {
                    while (channel.get() instanceof SignalingPacket packet) {
                        handleSignalPacket(packet);
                    }
                } else {
                    while (channel.get() instanceof BusinessPacket packet) {
                        handlerCoordinator.accept(session, packet);
                    }
                }
            } else if (selectionKey.isWritable()) {
                channel.write();
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

    @SuppressWarnings("unchecked")
    protected void onKeyNotValid(SelectionKey selectionKey) {
        SessionChannel<?, ClientSession> channel = (SessionChannel<?, ClientSession>) selectionKey.attachment();
        if (channel.getChannelType() == AbstractPacket.PacketType.Signaling) {
            closeChannel(channel, false);
        }
    }

    private void handleSignalPacket(SignalingPacket packet) throws Exception {
        switch (packet.getSignal()) {
            case SignalingPacket.Signal.BusinessChannelToken_Response -> {
                byte[] token = packet.getContent();
                Integer port = networkRouter.getPort(AbstractPacket.PacketType.Business);
                if (port == null) {
                    session.setBusinessChannelToken(token);
                } else {
                    link(AbstractPacket.PacketType.Business,
                            SocketChannel.open(new InetSocketAddress(networkRouter.getServerHost(), port)),
                            token);
                }
            }
            case SignalingPacket.Signal.BusinessChannelPort_Response -> {
                int port = ((ScalarSignal) packet).asInt();
                networkRouter.setPort(AbstractPacket.PacketType.Business, port);
                byte[] token = session.getBusinessChannelToken();
                if (token != null) {
                    link(AbstractPacket.PacketType.Business,
                            SocketChannel.open(new InetSocketAddress(networkRouter.getServerHost(), port)),
                            token);
                }
            }
            case SignalingPacket.Signal.BusinessChannelRate -> {
                long rate = ((ScalarSignal) packet).asLong();
                SessionChannel<?, ClientSession> businessChannel = session.getBusinessChannel();
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

    private void closeChannel(SessionChannel<?, ?> channel, boolean reconnect) {
        String channelInfo = channel.toString();
        logger.debug("closeChannel[{}]", channelInfo);
        AbstractPacket.PacketType type = channel.getChannelType();
        if (type == AbstractPacket.PacketType.Signaling) {
            channel.ownerSession().close();
            session = null;
        } else {
            try {
                channel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (!reconnect) {
            if (type == AbstractPacket.PacketType.Signaling) {
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
        if (type == AbstractPacket.PacketType.Signaling) {
            linkStatusManager.trigger(LinkStatusManager.Status.RECONNECTING);
            boolean success = false;
            for (int i = 1; i < 6; i++) {
                logger.debug("Signaling reconnection attempt [{}/5] to {}", i, networkRouter.getServerAddress());
                try {
                    if (link(AbstractPacket.PacketType.Signaling, SocketChannel.open(networkRouter.getServerAddress()), null)) {
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
            Integer port = networkRouter.getPort(AbstractPacket.PacketType.Business);
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
