package com.nebilux.nexalithic.server.io.session.signaling;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.infra.timer.TimerContext;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.channel.ChannelAccessRequestSignal;
import com.nebilux.nexalithic.core.session.SessionChannel;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.handshake.HandshakeContext;
import com.nebilux.nexalithic.server.io.session.ServerSessionLoop;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.session.ServerSession;
import com.nebilux.nexalithic.server.session.SessionRegistry;
import com.nebilux.nexalithic.server.session.access.ChannelAccessCoordinator;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.function.Function;

/**
 * 信令循环
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SignalingLoop extends ServerSessionLoop<SignalingPacket> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, SignalingLoop.class);
    public static final class Options extends ServerSessionLoop.Options {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {
            protected NexalithicOption<Integer> SlotCount() {
                return defineOptionLazy(context ->
                                Math.toIntExact(context.getOption(OPTIONS.MaxIdleTimeMillis) / context.getOption(OPTIONS.TimeWheel.TickMillis)) + 1
                        , OptionValidator.positive()
                );
            }
        };
        private Options(Class<?> holder) {
            super(holder);
        }
        @Override
        protected long MaxIdleTimeMillis_Value() {
            return 60_000L;
        }
    }
    private static final Modules MODULES = new Modules();
    private static final class Modules extends ModulesDefinition {
        private final NexalithicModule<TimeWheel<SessionChannel<SignalingPacket, ServerSession>>> TimeWheel = defineModule(TimeWheel.class);
        private Modules() {
            super(SignalingLoop.class);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(SignalingLoop.class);
    private final SessionRegistry sessionRegistry;
    private final ChannelAccessCoordinator channelAccessCoordinator;
    private final TimeWheel<SessionChannel<SignalingPacket, ServerSession>> timeWheel;
    private final Function<HandshakeContext, ServerSession> sessionFactory;

    public SignalingLoop(NexalithicBuilderContext context, ServiceUnit unit) throws IOException {
        super(context, OPTIONS);
        sessionRegistry = context.getModule(NexalithicServer.MODULES.SessionRegistry);
        channelAccessCoordinator = context.getModule(NexalithicServer.MODULES.ChannelAccessCoordinator);
        timeWheel = context.getModule(MODULES.TimeWheel, () -> {
            TimeWheel<SessionChannel<SignalingPacket, ServerSession>> timeWheel = new TimeWheel<>(
                    context.getOption(OPTIONS.TimeWheel.TickMillis),
                    context.getOption(OPTIONS.TimeWheel.SlotCount),
                    context.getOption(OPTIONS.TimeWheel.TickQuotaShift),
                    context.getOption(OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                    new GenericWrapperPool<>(
                            PoolStorageFactory.bounded(SpmcArrayQueue::new, context.getOption(OPTIONS.TimeWheel.WrapperPool_Capacity)),
                            PoolStrategyFactory.alwaysCreate(),
                            TimeWheel.ScheduleWrapper<SessionChannel<SignalingPacket, ServerSession>>::new
                    ),
                    SignalingLoop.class.getSimpleName()
            );
            timeWheel.start();
            return timeWheel;
        });
        TaskScheduler taskScheduler = context.getModule(NexalithicServer.MODULES.TaskScheduler);
        ServerSession.ChannelFactory channelFactory = new ServerSession.ChannelFactory(context, unit);
        sessionFactory = handoff -> new ServerSession(
                handoff.takeSessionKey(),
                handoff.takeSignalingSecretContext(),
                handoff.takeBusinessSecretContext(),
                channelFactory,
                taskScheduler,
                unit,
                channelAccessCoordinator
        );
    }

    @Override
    protected SessionChannel<SignalingPacket, ServerSession> onExecuteAcquireEvent(HandshakeContext handoff) {
        SocketChannel socketChannel = null;
        SessionChannel<SignalingPacket, ServerSession> sessionChannel = null;
        ServerSession session = null;
        boolean sessionRegistered = false;
        boolean channelOpened = false;
        try {
            socketChannel = handoff.takeChannel();
            SelectionKey selectionKey = registerSelectableChannel(socketChannel.configureBlocking(false), SelectionKey.OP_READ);
            session = sessionFactory.apply(handoff);
            sessionChannel = session.getSignalingChannel();
            sessionChannel.open(selectionKey, (InetSocketAddress) socketChannel.getRemoteAddress());
            channelOpened = true;
            socketChannel = null;
            if (!sessionRegistry.putSession(session)) {
                sessionChannel.close();
                session.close();
                return null;
            }
            sessionRegistered = true;
            timeWheel.schedule(sessionChannel, this);
            return sessionChannel;
        } catch (Exception failure) {
            logger.error("[{}] failed to acquire signaling channel", name, failure);
            if (sessionRegistered && session != null) {
                sessionRegistry.removeSession(session);
            }
            if (channelOpened && sessionChannel != null) {
                try {
                    sessionChannel.close();
                } catch (Exception closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            } else if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (Exception closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (session != null) {
                session.close();
            }
            return null;
        }
    }

    @Override
    protected void onChannelReady(SelectionKey selectionKey, SessionChannel<SignalingPacket, ServerSession> channel) {
        try {
            if (selectionKey.isReadable()) {
                if (channel.read() == -1) {
                    submitDisconnectEvent(channel, Event.Disconnect.Reason.REMOTE_CLOSED);
                }
                SignalingPacket packet;
                while ((packet = channel.get()) != null) {
                    handleSignalPacket(channel, packet);
                }
            } else if (selectionKey.isWritable()) {
                channel.write();
            } else {
                submitDisconnectEvent(channel, Event.Disconnect.Reason.PROTOCOL_VIOLATION);
            }
        } catch (InvalidAlgorithmParameterException | ShortBufferException | IllegalBlockSizeException |
                 BadPaddingException | InvalidKeyException e) {
            logger.warn("Channel[{}] onReadyEvent[{}] error", channel, name, e);
            submitDisconnectEvent(channel, Event.Disconnect.Reason.SECURITY_FAILURE);
        } catch (IOException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Channel[{}] onReadyEvent[{}] error", channel, name, e);
            }
            submitDisconnectEvent(channel, Event.Disconnect.Reason.IO_FAILURE);
        }
    }

    @Override
    protected void onExecuteDisconnectEvent(SessionChannel<SignalingPacket, ServerSession> channel, Event.Disconnect.Reason reason) {
        ServerSession session = channel.getOwnerSession();
        sessionRegistry.removeSession(session);
        session.close();
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<SessionChannel<SignalingPacket, ServerSession>> context) {
        SessionChannel<SignalingPacket, ServerSession> target = context.target();
        if (System.nanoTime() < getChannelExpiryTimeNanos(target)) {
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("heartbeat timeout [{}]", target.getOwnerSession().toString());
        }
        submitDisconnectEvent(target, Event.Disconnect.Reason.IDLE_TIMEOUT);
        return true;
    }

    private void handleSignalPacket(SessionChannel<SignalingPacket, ServerSession> channel, SignalingPacket packet) {
        if (!switch (packet.getSignal()) {
            case SignalingPacket.Signal.ChannelAccess_Request -> handleChannelAccessRequest(channel, (ChannelAccessRequestSignal) packet);
            case SignalingPacket.Signal.BusinessChannelRate -> {
                long rate = ((ScalarSignal) packet).asLong();
                channel.getOwnerSession().getBusinessChannel().updateWriteRate(rate);
                yield true;
            }
            default -> true;
        }) {
            logger.warn("ServerSessionChannel[{}] signalingPacket overflow", channel);
            submitDisconnectEvent(channel, Event.Disconnect.Reason.INTERNAL_FAILURE);
        }
    }

    private boolean handleChannelAccessRequest(SessionChannel<SignalingPacket, ServerSession> channel, ChannelAccessRequestSignal request) {
        ChannelAccessCoordinator.Result result = channelAccessCoordinator.issue(channel.getOwnerSession(), request.getKind(), channel.getRemoteAddress().getAddress());
        return switch (result) {
            case ISSUED -> true;
            case UNSUPPORTED_KIND -> {
                logger.warn(
                        "Unsupported channel access request [{}] from [{}]",
                        request.getKind(),
                        channel.getRemoteAddress()
                );
                submitDisconnectEvent(channel, Event.Disconnect.Reason.PROTOCOL_VIOLATION);
                yield true;
            }
            case ROUTE_UNAVAILABLE -> {
                logger.warn(
                        "No [{}] channel route is available for [{}]",
                        request.getKind(),
                        channel.getRemoteAddress()
                );
                submitDisconnectEvent(channel, Event.Disconnect.Reason.INTERNAL_FAILURE);
                yield true;
            }
            case SIGNALING_QUEUE_FULL -> false;
        };
    }
}
