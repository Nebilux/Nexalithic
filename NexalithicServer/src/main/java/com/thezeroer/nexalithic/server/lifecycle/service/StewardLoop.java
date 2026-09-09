package com.thezeroer.nexalithic.server.lifecycle.service;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.module.ModulesDefinition;
import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.thezeroer.nexalithic.core.infra.timer.TimeWheel;
import com.thezeroer.nexalithic.core.infra.timer.TimerContext;
import com.thezeroer.nexalithic.core.infra.timer.TimerCoordinator;
import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.thezeroer.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.thezeroer.nexalithic.core.model.packet.signaling.TokenSignal;
import com.thezeroer.nexalithic.core.session.SessionKey;
import com.thezeroer.nexalithic.server.NexalithicServer;
import com.thezeroer.nexalithic.server.lifecycle.handshake.PendingChannel;
import com.thezeroer.nexalithic.server.lifecycle.service.session.ServerSession;
import com.thezeroer.nexalithic.server.lifecycle.service.session.ServerSessionChannel;
import com.thezeroer.nexalithic.server.manager.NetworkRouter;
import com.thezeroer.nexalithic.server.manager.SessionsManager;
import org.jctools.queues.SpmcArrayQueue;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.channels.SelectionKey;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 主选择器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/06
 * @version 1.0.0
 */
public class StewardLoop extends ServiceLoop<SignalingPacket> implements TimerCoordinator<ServerSession> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, StewardLoop.class);
    public static final class Options extends ServiceLoop.Options {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {
            protected NexalithicOption<Integer> SlotCount() {
                return defineOptionLazy(context ->
                                Math.toIntExact(context.getOption(OPTIONS.HeartBeat_MaxIntervalMillis) / context.getOption(OPTIONS.TimeWheel.TickMillis)) + 1
                        , OptionValidator.positive()
                );
            }
        };
        public final NexalithicOption<Long> HeartBeat_MaxIntervalMillis = defineOption(
                60_000L, OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<TimeWheel<ServerSession>> TimeWheel = defineModule(TimeWheel.class);
        private Modules() {
            super(StewardLoop.class);
        }
    }
    private final SessionsManager sessionsManager;
    private final NetworkRouter networkRouter;
    private final TimeWheel<ServerSession> timeWheel;
    private final SecureRandom secureRandom = new SecureRandom();
    private final Function<PendingChannel, ServerSession> sessionFactory;

    public StewardLoop(NexalithicBuilderContext context, ServiceUnit unit) throws IOException {
        super(context, OPTIONS);
        sessionsManager = context.getModule(NexalithicServer.MODULES.SessionsManager);
        networkRouter = context.getModule(NexalithicServer.MODULES.NetworkRouter);
        timeWheel = context.getModule(MODULES.TimeWheel, () -> {
            TimeWheel<ServerSession> timeWheel = new TimeWheel<>(
                    context.getOption(OPTIONS.TimeWheel.TickMillis),
                    context.getOption(OPTIONS.TimeWheel.SlotCount),
                    context.getOption(OPTIONS.TimeWheel.TickQuotaShift),
                    context.getOption(OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                    new GenericWrapperPool<>(
                            PoolStorageFactory.bounded(SpmcArrayQueue::new, context.getOption(OPTIONS.TimeWheel.WrapperPool_Capacity)),
                            PoolStrategyFactory.alwaysCreate(),
                            TimeWheel.ScheduleWrapper<ServerSession>::new
                    ),
                    StewardLoop.class.getSimpleName()
            );
            timeWheel.start();
            return timeWheel;
        });
        ServerSession.ServerChannelFactory channelFactory = new ServerSession.ServerChannelFactory(context, this);
        TaskScheduler taskScheduler = context.getModule(NexalithicServer.MODULES.TaskScheduler);
        ServerSession.Constant sessionConstant = context.getConstant(ServerSession.class, ServerSession.Constant.class, () -> new ServerSession.Constant(
                TimeUnit.NANOSECONDS.convert(context.getOption(OPTIONS.HeartBeat_MaxIntervalMillis), TimeUnit.MILLISECONDS)
        ));
        sessionFactory = channel -> new ServerSession(
                channel.getSessionKey(),
                channel.getSignalingSecretContext(),
                channel.getBusinessSecretContext(),
                channelFactory,
                taskScheduler,
                sessionConstant,
                unit
        );
    }

    public boolean prepareChannelAccess(ServerSession session, AbstractPacket.PacketType type, InetAddress remoteAddress) {
        SessionKey.Immutable sessionKey = new SessionKey.Immutable(secureRandom.nextLong(), secureRandom.nextLong());
        sessionsManager.relateChannelToken(sessionKey, session);
        return session.pushSignalingPacket(
                ScalarSignal.ofInt(SignalingPacket.Signal.BusinessChannelPort_Response, networkRouter.choosePort(type, remoteAddress)),
                new TokenSignal(sessionKey)
        ) == 0;
    }

    @Override
    protected boolean onAsyncEvent() {
        dispatchQueue.drain(channel -> {
            try {
                SelectionKey selectionKey = channel.getSocketChannel().configureBlocking(false).register(selector, SelectionKey.OP_READ);
                ServerSession session = sessionFactory.apply(channel);
                selectionKey.attach(session.getSignalingChannel().updateChannel(selectionKey));
                if (!sessionsManager.putSession(session)) {
                    closeChannel(session.getSignalingChannel());
                    return;
                }
                session.updateLastActiveTimeNanos(System.nanoTime());
                timeWheel.schedule(session, this);
            } catch (IOException ignored) {
            } finally {
                channel.recycle();
            }
        }, CONSTANT.DrainLimit());
        return dispatchQueue.isEmpty();
    }

    @Override
    protected void onReadyEvent(SelectionKey key, ServerSessionChannel<SignalingPacket> channel) {
        try {
            if (key.isReadable()) {
                if (channel.read() == -1) {
                    closeChannel(channel);
                }
                SignalingPacket packet;
                while ((packet = channel.get()) != null) {
                    handleSignalPacket(channel, packet);
                }
            } else if (key.isWritable()) {
                if (channel.write() == -1) {
                    key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                }
            } else {
                closeChannel(channel);
            }
        } catch (InvalidAlgorithmParameterException | ShortBufferException | IllegalBlockSizeException |
                 BadPaddingException | InvalidKeyException e) {
            logger.warn("Channel[{}] onReadyEvent[{}] error", channel, name, e);
            closeChannel(channel);
        } catch (IOException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Channel[{}] onReadyEvent[{}] error", channel, name, e);
            }
            closeChannel(channel);
        }
    }

    @Override
    public long getExpiryTimeNanos(TimerContext<ServerSession> context) {
        return context.target().getExpiryTimeNanos();
    }

    @Override
    public boolean isCancelled(TimerContext<ServerSession> context) {
        return context.target().getLastActiveTimeNanos() == -1;
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<ServerSession> context) {
        ServerSession target = context.target();
        if (System.nanoTime() < target.getExpiryTimeNanos()) {
            return false;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("heartbeat timeout [{}]", target.toString());
        }
        closeChannel(target.getSignalingChannel());
        return true;
    }

    private void handleSignalPacket(ServerSessionChannel<SignalingPacket> channel, SignalingPacket packet) {
        if (!switch (packet.getSignal()) {
            case SignalingPacket.Signal.BusinessChannelPort_Request -> channel.session().pushSignalingPacket(
                    ScalarSignal.ofInt(SignalingPacket.Signal.BusinessChannelPort_Response,
                            networkRouter.choosePort(AbstractPacket.PacketType.BUSINESS, channel.getRemoteAddress().getAddress())));
            case SignalingPacket.Signal.BusinessChannelToken_Request -> {
                SessionKey.Immutable sessionKey = new SessionKey.Immutable(secureRandom.nextLong(), secureRandom.nextLong());
                ServerSession session = channel.session();
                sessionsManager.relateChannelToken(sessionKey, session);
                yield session.pushSignalingPacket(new TokenSignal(sessionKey));
            }
            case SignalingPacket.Signal.BusinessChannelRate -> {
                long rate = ((ScalarSignal) packet).asLong();
                channel.session().getBusinessChannel().updateWriteRate(rate);
                yield true;
            }
            default -> true;
        }) {
            logger.warn("ServerSessionChannel[{}] signalingPacket overflow", channel);
            closeChannel(channel);
        }
    }

    private void closeChannel(ServerSessionChannel<?> channel) {
        ServerSession session = channel.session();
        if (super.closeChannel(channel)) {
            sessionsManager.removeSession(session);
        }
        session.close();
    }
}
