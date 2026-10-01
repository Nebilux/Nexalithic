package com.nebilux.nexalithic.server.io.session.business;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.rate.DynamicRateController;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.SessionChannel;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.handshake.HandshakeContext;
import com.nebilux.nexalithic.server.io.session.ServerSessionLoop;
import com.nebilux.nexalithic.server.messaging.ServerHandlerCoordinator;
import com.nebilux.nexalithic.server.session.ServerSession;
import org.jctools.queues.SpmcArrayQueue;
import org.jctools.queues.SpscArrayQueue;
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
import java.util.concurrent.TimeUnit;

/**
 * 业务循环
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BusinessLoop extends ServerSessionLoop<BusinessPacket> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, BusinessLoop.class);
    public static final class Options extends ServerSessionLoop.Options {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {
            protected NexalithicOption<Integer> SlotCount() {
                return defineOptionLazy(context ->
                                Math.toIntExact(context.getOption(OPTIONS.MaxIdleTimeMillis) / context.getOption(OPTIONS.TimeWheel.TickMillis)) + 1
                        , OptionValidator.positive()
                );
            }
        };
        public final DynamicRateController.Options DynamicRateController = new DynamicRateController.Options(holder) {};
        public final NexalithicOption<Integer> RateUpdateQueue_Capacity = defineOption(
                1024, OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
        @Override
        protected long MaxIdleTimeMillis_Value() {
            return 600_000L;
        }
    }
    private static final Modules MODULES = new Modules();
    private static final class Modules extends ModulesDefinition {
        private final NexalithicModule<TimeWheel<SessionChannel<BusinessPacket, ServerSession>>> TimeWheel = defineModule(TimeWheel.class);
        private Modules() {
            super(BusinessLoop.class);
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(BusinessLoop.class);
    private final ServerHandlerCoordinator handlerCoordinator;
    private final TimeWheel<SessionChannel<BusinessPacket, ServerSession>> timeWheel;
    private final SpscArrayQueue<SessionChannel<BusinessPacket, ServerSession>> rateUpdateQueue;
    private final DynamicRateController dynamicRateController;
    private final boolean dynamicRateEnable;
    private final long dynamicRateTickNanos;
    private long lastDynamicRateTickNanos;

    public BusinessLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        handlerCoordinator = context.getModule(NexalithicServer.MODULES.HandlerCoordinator);
        timeWheel = context.getModule(MODULES.TimeWheel, () -> {
            TimeWheel<SessionChannel<BusinessPacket, ServerSession>> timeWheel = new TimeWheel<>(
                    context.getOption(OPTIONS.TimeWheel.TickMillis),
                    context.getOption(OPTIONS.TimeWheel.SlotCount),
                    context.getOption(OPTIONS.TimeWheel.TickQuotaShift),
                    context.getOption(OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                    new GenericWrapperPool<>(
                            PoolStorageFactory.bounded(SpmcArrayQueue::new, context.getOption(OPTIONS.TimeWheel.WrapperPool_Capacity)),
                            PoolStrategyFactory.alwaysCreate(),
                            TimeWheel.ScheduleWrapper<SessionChannel<BusinessPacket, ServerSession>>::new
                    ),
                    BusinessLoop.class.getSimpleName()
            );
            timeWheel.start();
            return timeWheel;
        });
        rateUpdateQueue = new SpscArrayQueue<>(context.getOption(OPTIONS.RateUpdateQueue_Capacity));
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
        dynamicRateEnable = context.getOption(OPTIONS.DynamicRateController.Enable);
        dynamicRateTickNanos = TimeUnit.NANOSECONDS.convert(context.getOption(OPTIONS.DynamicRateController.TickMillis), TimeUnit.MILLISECONDS);
        lastDynamicRateTickNanos = System.nanoTime();
        isDrainCondition(rateUpdateQueue::isEmpty);
        drainAsyncEventsCondition(() -> {
            if (dynamicRateEnable) {
                rateUpdateQueue.drain(SessionChannel::applyRate, CONSTANT.DispatchQueue_DrainLimit());
                long now = System.nanoTime();
                if (now - lastDynamicRateTickNanos >= dynamicRateTickNanos) {
                    long interval = now - lastDynamicRateTickNanos;
                    lastDynamicRateTickNanos = now;
                    for (SelectionKey key : registeredKeys()) {
                        if (!key.isValid() || !(key.attachment() instanceof SessionChannel<?, ?> businessChannel)) {
                            continue;
                        }
                        long targetRate = businessChannel.evaluateDynamicRate(interval, now, dynamicRateController);
                        if (targetRate > 0) {
                            businessChannel.ownerSession().setRemoteBusinessChannelWriteRate(targetRate);
                        }
                    }
                }
            }
            return rateUpdateQueue.isEmpty();
        });
    }

    @Override
    public void postRateUpdate(SessionChannel<BusinessPacket, ServerSession> channel) {
        rateUpdateQueue.offer(channel);
        wakeup();
    }

    @Override
    protected boolean onExecuteAcquireEvent(HandshakeContext handoff) {
        try {
            SocketChannel socketChannel = handoff.takeChannel();
            SelectionKey selectionKey = registerSelectableChannel(socketChannel.configureBlocking(false), SelectionKey.OP_READ);
            SessionChannel<BusinessPacket, ServerSession> sessionChannel = handoff.takeTargetSession().getBusinessChannel();
            sessionChannel.open(socketChannel, selectionKey, (InetSocketAddress) socketChannel.getRemoteAddress());
            if (!sessionChannel.fragmenterIsEmpty()) {
                sessionChannel.updateInterest(SelectionKey.OP_WRITE, true);
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
    protected boolean onExecuteDisconnectEvent(SessionChannel<BusinessPacket, ServerSession> channel, Event.Disconnect.Reason reason) {
        return true;
    }

    @Override
    protected void onChannelReady(SelectionKey selectionKey, SessionChannel<BusinessPacket, ServerSession> channel) {
        try {
            if (selectionKey.isReadable()) {
                if (channel.read() == -1) {
                    submitDisconnectEvent(channel, Event.Disconnect.Reason.REMOTE_CLOSED);
                }
                BusinessPacket packet;
                while ((packet = channel.get()) != null) {
                    handlerCoordinator.accept(channel.ownerSession(), packet);
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
}
