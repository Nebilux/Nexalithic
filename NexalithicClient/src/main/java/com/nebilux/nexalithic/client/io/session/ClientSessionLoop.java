package com.nebilux.nexalithic.client.io.session;

import com.nebilux.nexalithic.client.NexalithicClient;
import com.nebilux.nexalithic.client.messaging.ClientHandlerCoordinator;
import com.nebilux.nexalithic.client.session.ClientSession;
import com.nebilux.nexalithic.client.session.SessionManager;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.rate.DynamicRateController;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.BareSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.channel.ChannelAccessResponseSignal;
import com.nebilux.nexalithic.core.session.SessionChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 客户端会话循环
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClientSessionLoop extends SessionLoop<ClientChannelHandoff, SessionChannel<?, ClientSession>> {
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

    private static final Logger logger = LoggerFactory.getLogger(ClientSessionLoop.class);
    private record Constant(long HeartBeat_IntervalNanos, boolean DynamicRate_Enable, long DynamicRate_TickNanos) {}
    private final Constant CONSTANT;
    private final SessionManager sessionManager;
    private final ClientHandlerCoordinator handlerCoordinator;
    private final DynamicRateController dynamicRateController;
    private final Function<ClientChannelHandoff.Signaling, ClientSession> sessionFactory;
    private long lastDynamicRateTickNanos;

    public ClientSessionLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        CONSTANT = new Constant(
                TimeUnit.NANOSECONDS.convert(context.getOption(OPTIONS.MaxIdleTimeMillis), TimeUnit.MILLISECONDS),
                context.getOption(OPTIONS.DynamicRateController.Enable),
                TimeUnit.NANOSECONDS.convert(context.getOption(OPTIONS.DynamicRateController.TickMillis), TimeUnit.MILLISECONDS)
        );
        sessionManager = context.getModule(NexalithicClient.MODULES.SessionManager);
        handlerCoordinator = context.getModule(NexalithicClient.MODULES.HandlerCoordinator);
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
        TaskScheduler taskScheduler = context.getModule(NexalithicClient.MODULES.TaskScheduler);
        ClientSession.ChannelFactory channelFactory = new ClientSession.ChannelFactory(context, this);
        sessionFactory = handoff -> new ClientSession(
                handoff.sessionKey(), handoff.signalingSecretKey(), handoff.businessSecretKey(),
                channelFactory, taskScheduler, sessionManager
        );
        lastDynamicRateTickNanos = System.nanoTime();
        drainAsyncEventsCondition(() -> {
            if (sessionManager.isShuttingDown()) {
                sessionManager.tryFinishGracefulShutdown();
                return true;
            }
            ClientSession session = sessionManager.getCurrentSession();
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
            return true;
        });
    }

    @Override
    protected SessionChannel<?, ClientSession> onExecuteAcquireEvent(ClientChannelHandoff handoff) throws Exception {
        if (sessionManager.isShuttingDown() || !sessionManager.isAccepting()) {
            return null;
        }
        if (handoff instanceof ClientChannelHandoff.Business business) {
            if (!sessionManager.isCurrent(business.targetSession(), handoff.stamp())) {
                return null;
            }
        } else if (!sessionManager.isCurrent(handoff.stamp())) {
            return null;
        }
        return switch (handoff) {
            case ClientChannelHandoff.Signaling signaling -> acquireSignalingChannel(signaling);
            case ClientChannelHandoff.Business business -> acquireBusinessChannel(business);
        };
    }

    @Override
    protected void onChannelAcquired(ClientChannelHandoff handoff, SessionChannel<?, ClientSession> channel) {
        sessionManager.onChannelAcquired(handoff, channel);
        logger.debug("[{}] channel acquisition succeeded", channel.getKind());
    }

    @Override
    protected void onChannelAcquireFailed(ClientChannelHandoff handoff, Throwable failure) {
        sessionManager.onChannelAcquireFailed(handoff, failure);
    }

    @Override
    protected boolean onExecuteReleaseEvent(SessionChannel<?, ClientSession> channel) {
        SelectionKey key = channel.getSelectionKey();
        if (key != null) {
            key.cancel();
        }
        return true;
    }

    @Override
    protected void onExecuteDisconnectEvent(SessionChannel<?, ClientSession> channel, Event.Disconnect.Reason reason) {
        sessionManager.onChannelDisconnected(channel, reason);
    }

    @Override
    protected void onChannelReady(SelectionKey selectionKey, SessionChannel<?, ClientSession> channel) {
        try {
            if (selectionKey.isReadable()) {
                if (channel.read() == -1) {
                    submitDisconnectEvent(channel, Event.Disconnect.Reason.REMOTE_CLOSED);
                    return;
                }
                ClientSession ownerSession = channel.getOwnerSession();
                if (channel.getKind() == NexalithicChannel.Kind.Packet_Signaling) {
                    while (channel.get() instanceof SignalingPacket packet) {
                        handleSignalPacket(ownerSession, packet);
                    }
                } else {
                    while (channel.get() instanceof BusinessPacket packet) {
                        handlerCoordinator.accept(ownerSession, packet);
                    }
                }
            } else if (selectionKey.isWritable()) {
                channel.write();
            } else {
                submitDisconnectEvent(channel, Event.Disconnect.Reason.REMOTE_CLOSED);
            }
        } catch (IOException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Channel[{}] onReadyEvent[{}] error", channel.toString(), name, e);
            }
            submitDisconnectEvent(channel, Event.Disconnect.Reason.IO_FAILURE);
        } catch (Exception e) {
            logger.warn("Channel[{}] onReadyEvent[{}] error", channel.toString(), name, e);
            submitDisconnectEvent(channel, Event.Disconnect.Reason.PROTOCOL_VIOLATION);
        }
    }

//    @SuppressWarnings("unchecked")
//    protected void onKeyNotValid(SelectionKey selectionKey) {
//        SessionChannel<?, ClientSession> channel = (SessionChannel<?, ClientSession>) selectionKey.attachment();
//        if (channel.getChannelType() == AbstractPacket.PacketType.Signaling) {
//            disconnectChannel(channel, Event.Disconnect.Reason.REMOTE_CLOSED);
//        }
//    }

    private void handleSignalPacket(ClientSession session, SignalingPacket packet) {
        switch (packet.getSignal()) {
            case SignalingPacket.Signal.ChannelAccess_Response -> sessionManager.onChannelAccess(session, (ChannelAccessResponseSignal) packet);
            case SignalingPacket.Signal.BusinessChannelRate -> {
                long rate = ((ScalarSignal) packet).asLong();
                SessionChannel<?, ClientSession> businessChannel = session.getBusinessChannel();
                businessChannel.updateWriteRate(rate);
                businessChannel.applyRate();
            }
        }
    }

    private SessionChannel<?, ClientSession> acquireSignalingChannel(ClientChannelHandoff.Signaling handoff) throws Exception {
        SessionChannel<?, ClientSession> channel = sessionFactory.apply(handoff).getSignalingChannel();
        openTransferredChannel(handoff, channel);
        return channel;
    }

    private SessionChannel<?, ClientSession> acquireBusinessChannel(ClientChannelHandoff.Business handoff) throws Exception {
        ClientSession target = handoff.targetSession();
        SessionChannel<?, ClientSession> channel = target.getBusinessChannel();
        openTransferredChannel(handoff, channel);
        return channel;
    }

    private void openTransferredChannel(ClientChannelHandoff handoff, SessionChannel<?, ClientSession> channel) throws Exception {
        SocketChannel socketChannel = handoff.takeChannel();
        try {
            socketChannel.configureBlocking(false);
            SelectionKey selectionKey = registerSelectableChannel(socketChannel, SelectionKey.OP_READ);
            channel.open(selectionKey, (InetSocketAddress) socketChannel.getRemoteAddress());
            if (!channel.fragmenterIsEmpty()) {
                channel.updateInterest(SelectionKey.OP_WRITE, true);
            }
        } catch (Exception | Error failure) {
            try {
                channel.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            try {
                socketChannel.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }
}
