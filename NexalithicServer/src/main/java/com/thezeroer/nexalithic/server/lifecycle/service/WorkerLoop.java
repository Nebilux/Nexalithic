package com.thezeroer.nexalithic.server.lifecycle.service;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.module.ModulesDefinition;
import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.rate.DynamicRateController;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.thezeroer.nexalithic.core.infra.timer.TimeWheel;
import com.thezeroer.nexalithic.core.infra.timer.TimerContext;
import com.thezeroer.nexalithic.core.infra.timer.TimerCoordinator;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.session.channel.SessionChannel;
import com.thezeroer.nexalithic.server.NexalithicServer;
import com.thezeroer.nexalithic.server.lifecycle.service.session.ServerSessionChannel;
import com.thezeroer.nexalithic.server.messaging.ServerHandlerCoordinator;
import org.jctools.queues.SpmcArrayQueue;
import org.jctools.queues.SpscArrayQueue;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * 从属选择器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/06
 * @version 1.0.0
 */
public class WorkerLoop extends ServiceLoop<BusinessPacket> implements TimerCoordinator<ServerSessionChannel<BusinessPacket>> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, WorkerLoop.class);
    public static final class Options extends ServiceLoop.Options {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {
            protected NexalithicOption<Integer> SlotCount() {
                return NexalithicOption.create((Function<NexalithicBuilderContext, Integer>) context ->
                                Math.toIntExact(context.getOption(OPTIONS.MaxIdleTimeMillis) / context.getOption(OPTIONS.TimeWheel.TickMillis)) + 1
                        , OptionValidator.positive()
                );
            }
        };
        public final DynamicRateController.Options DynamicRateController = new DynamicRateController.Options(holder) {};
        public final NexalithicOption<Long> MaxIdleTimeMillis = NexalithicOption.create(
                600_000L, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> RateUpdateQueue_Capacity = NexalithicOption.create(
                1024, OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    public static final class Modules implements ModulesDefinition {
        public static final NexalithicModule<TimeWheel<ServerSessionChannel<BusinessPacket>>> TimeWheel = NexalithicModule.create("WorkerLoop_TimeWheel", TimeWheel.class);
    }
    private final ServerHandlerCoordinator handlerCoordinator;
    private final TimeWheel<ServerSessionChannel<BusinessPacket>> timeWheel;
    private final SpscArrayQueue<ServerSessionChannel<?>> rateUpdateQueue;
    private final DynamicRateController dynamicRateController;
    private final boolean dynamicRateEnable;
    private final long dynamicRateTickNanos;
    private long lastDynamicRateTickNanos;

    public WorkerLoop(NexalithicBuilderContext context) throws IOException {
        super(context, OPTIONS);
        handlerCoordinator = context.getModule(NexalithicServer.Modules.HandlerCoordinator);
        timeWheel = context.getModule(Modules.TimeWheel, () -> {
            TimeWheel<ServerSessionChannel<BusinessPacket>> timeWheel = new TimeWheel<>(
                    context.getOption(OPTIONS.TimeWheel.TickMillis),
                    context.getOption(OPTIONS.TimeWheel.SlotCount),
                    context.getOption(OPTIONS.TimeWheel.TickQuotaShift),
                    context.getOption(OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                    new GenericWrapperPool<>(
                            PoolStorageFactory.bounded(SpmcArrayQueue::new, context.getOption(OPTIONS.TimeWheel.WrapperPool_Capacity)),
                            PoolStrategyFactory.alwaysCreate(),
                            TimeWheel.ScheduleWrapper<ServerSessionChannel<BusinessPacket>>::new
                    ),
                    WorkerLoop.class.getSimpleName()
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
    }

    @Override
    public void postRateUpdate(SessionChannel<?, ?> channel) {
        rateUpdateQueue.offer((ServerSessionChannel<?>) channel);
        wakeupIfNeeded();
    }

    @Override
    protected boolean onAsyncEvent() {
        dispatchQueue.drain(channel -> {
            try {
                SelectionKey selectionKey = channel.getSocketChannel().configureBlocking(false).register(selector, SelectionKey.OP_READ);
                ServerSessionChannel<BusinessPacket> businessChannel = channel.getSession().getBusinessChannel();
                selectionKey.attach(businessChannel.updateChannel(this, selectionKey));
                if (!businessChannel.fragmenterIsEmpty() && businessChannel.updateChannelInterest(SelectionKey.OP_WRITE, true)) {
                    businessChannel.applyTargetInterest();
                }
                businessChannel.updateLastActiveTimeNanos(System.nanoTime());
                timeWheel.schedule(businessChannel, this);
            } catch (IOException ignored) {
            } finally {
                channel.recycle();
            }
        }, CONSTANT.DrainLimit());
        rateUpdateQueue.drain(SessionChannel::applyRate, CONSTANT.DrainLimit());
        if (dynamicRateEnable) {
            long now = System.nanoTime();
            if (now - lastDynamicRateTickNanos >= dynamicRateTickNanos) {
                long interval = now - lastDynamicRateTickNanos;
                lastDynamicRateTickNanos = now;
                for (SelectionKey key : selector.keys()) {
                    if (!key.isValid() || !(key.attachment() instanceof ServerSessionChannel<?> businessChannel)) {
                        continue;
                    }
                    long targetRate = businessChannel.evaluateDynamicRate(interval, now, dynamicRateController);
                    if (targetRate > 0) {
                        businessChannel.session().setRemoteBusinessChannelWriteRate(targetRate);
                    }
                }
            }
        }
        return dispatchQueue.isEmpty() && rateUpdateQueue.isEmpty();
    }

    @Override
    protected void onReadyEvent(SelectionKey key, ServerSessionChannel<BusinessPacket> channel) {
        try {
            if (key.isReadable()) {
                if (channel.read() == -1) {
                    closeChannel(channel);
                }
                BusinessPacket packet;
                while ((packet = channel.get()) != null) {
                    handlerCoordinator.accept(channel.session(), packet);
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
    protected void onShuttingDown() {
        for (SelectionKey key : selector.keys()) {
            try {
                key.channel().close();
            } catch (IOException ignored) {}
        }
    }

    @Override
    public long getExpiryTimeNanos(TimerContext<ServerSessionChannel<BusinessPacket>> context) {
        return context.target().getExpiryTimeNanos();
    }

    @Override
    public boolean isCancelled(TimerContext<ServerSessionChannel<BusinessPacket>> context) {
        return context.target().getLastActiveTimeNanos() == -1;
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<ServerSessionChannel<BusinessPacket>> context) {
        ServerSessionChannel<BusinessPacket> target = context.target();
        if (System.nanoTime() < target.getExpiryTimeNanos()) {
            return false;
        }
        closeChannel(target);
        return true;
    }
}
