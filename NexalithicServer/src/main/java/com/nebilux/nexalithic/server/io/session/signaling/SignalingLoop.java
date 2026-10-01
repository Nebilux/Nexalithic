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
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.TokenSignal;
import com.nebilux.nexalithic.core.session.SessionChannel;
import com.nebilux.nexalithic.core.session.SessionKey;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.handshake.HandshakeContext;
import com.nebilux.nexalithic.server.io.session.ServerSessionLoop;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.manager.NetworkRouter;
import com.nebilux.nexalithic.server.manager.SessionsManager;
import com.nebilux.nexalithic.server.session.ServerChannelFactory;
import com.nebilux.nexalithic.server.session.ServerSession;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
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
    private final SessionsManager sessionsManager;
    private final NetworkRouter networkRouter;
    private final TimeWheel<SessionChannel<SignalingPacket, ServerSession>> timeWheel;
    private final SecureRandom secureRandom = new SecureRandom();
    private final Function<HandshakeContext, ServerSession> sessionFactory;

    public SignalingLoop(NexalithicBuilderContext context, ServiceUnit unit) throws IOException {
        super(context, OPTIONS);
        sessionsManager = context.getModule(NexalithicServer.MODULES.SessionsManager);
        networkRouter = context.getModule(NexalithicServer.MODULES.NetworkRouter);
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
        ServerChannelFactory channelFactory = new ServerChannelFactory(context, unit);
        TaskScheduler taskScheduler = context.getModule(NexalithicServer.MODULES.TaskScheduler);
        sessionFactory = channel -> new ServerSession(
                channel.takeSessionKey(),
                channel.takeSignalingSecretContext(),
                channel.takeBusinessSecretContext(),
                channelFactory,
                taskScheduler,
                unit
        );
    }

    public boolean prepareChannelAccess(ServerSession session, InetAddress remoteAddress) {
        SessionKey.Immutable sessionKey = new SessionKey.Immutable(secureRandom.nextLong(), secureRandom.nextLong());
        sessionsManager.relateChannelToken(sessionKey, session);
        return session.pushSignalingPacket(
                ScalarSignal.ofInt(SignalingPacket.Signal.BusinessChannelPort_Response, networkRouter.choosePort(NexalithicChannel.Kind.Packet_Business, remoteAddress)),
                new TokenSignal(sessionKey)
        ) == 0;
    }

    @Override
    protected boolean onExecuteAcquireEvent(HandshakeContext handoff) {
        try {
            SocketChannel socketChannel = handoff.takeChannel();
            SelectionKey selectionKey = registerSelectableChannel(socketChannel.configureBlocking(false), SelectionKey.OP_READ);
            ServerSession session = sessionFactory.apply(handoff);
            SessionChannel<SignalingPacket, ServerSession> sessionChannel = session.getSignalingChannel();
            sessionChannel.open(socketChannel, selectionKey, (InetSocketAddress) socketChannel.getRemoteAddress());
            if (!sessionsManager.putSession(session)) {
                submitDisconnectEvent(sessionChannel, Event.Disconnect.Reason.INTERNAL_FAILURE);
                return false;
            }
            sessionChannel.updateLastActiveTimeNanos(System.nanoTime());
            timeWheel.schedule(sessionChannel, this);
        } catch (IOException ignored) {
        } finally {
            handoff.recycle();
        }
        return true;
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
    protected boolean onExecuteDisconnectEvent(SessionChannel<SignalingPacket, ServerSession> channel, Event.Disconnect.Reason reason) {
        ServerSession session = channel.ownerSession();
        sessionsManager.removeSession(session);
        session.close();
        return true;
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<SessionChannel<SignalingPacket, ServerSession>> context) {
        SessionChannel<SignalingPacket, ServerSession> target = context.target();
        if (System.nanoTime() < getChannelExpiryTimeNanos(target)) {
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("heartbeat timeout [{}]", target.ownerSession().toString());
        }
        submitDisconnectEvent(target, Event.Disconnect.Reason.IDLE_TIMEOUT);
        return true;
    }

    private void handleSignalPacket(SessionChannel<SignalingPacket, ServerSession> channel, SignalingPacket packet) {
        if (!switch (packet.getSignal()) {
            case SignalingPacket.Signal.BusinessChannelPort_Request -> channel.ownerSession().pushSignalingPacket(
                    ScalarSignal.ofInt(SignalingPacket.Signal.BusinessChannelPort_Response,
                            networkRouter.choosePort(NexalithicChannel.Kind.Packet_Business, channel.getRemoteAddress().getAddress())));
            case SignalingPacket.Signal.BusinessChannelToken_Request -> {
                SessionKey.Immutable sessionKey = new SessionKey.Immutable(secureRandom.nextLong(), secureRandom.nextLong());
                ServerSession session = channel.ownerSession();
                sessionsManager.relateChannelToken(sessionKey, session);
                yield session.pushSignalingPacket(new TokenSignal(sessionKey));
            }
            case SignalingPacket.Signal.BusinessChannelRate -> {
                long rate = ((ScalarSignal) packet).asLong();
                channel.ownerSession().getBusinessChannel().updateWriteRate(rate);
                yield true;
            }
            default -> true;
        }) {
            logger.warn("ServerSessionChannel[{}] signalingPacket overflow", channel);
            submitDisconnectEvent(channel, Event.Disconnect.Reason.INTERNAL_FAILURE);
        }
    }
}
