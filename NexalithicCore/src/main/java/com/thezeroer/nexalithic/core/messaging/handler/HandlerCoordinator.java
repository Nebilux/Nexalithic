package com.thezeroer.nexalithic.core.messaging.handler;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.module.ModulesDefinition;
import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.executor.BlockingTaskQueue;
import com.thezeroer.nexalithic.core.infra.executor.FixedTaskExecutor;
import com.thezeroer.nexalithic.core.infra.executor.TypedThreadFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.WrapperPool;
import com.thezeroer.nexalithic.core.messaging.handler.mapping.HandlerRegistry;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务包分发器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/16
 * @version 1.0.0
 */
public abstract class HandlerCoordinator<
        S extends NexalithicSession<?, ?, ?>,
        HC extends HandlerContext<S>,
        HR extends HandlerContext.Recyclable<S, HC, HR>
        > {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, HandlerCoordinator.class);
    public static class Options extends OptionsDefinition {
        public final FixedTaskExecutor.Options FixedTaskExecutor = new FixedTaskExecutor.Options(holder) {};
        public final NexalithicOption<Integer> HandlerContextPool_Capacity = defineOption(
                HandlerContextPool_Capacity_DefaultValue(), OptionValidator.positive()
        );
        public final NexalithicOption<Double> HandlerContextPool_PrefillRatio = defineOption(
                HandlerContextPool_PrefillRatio_DefaultValue(), OptionValidator.unitInterval()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
        protected Integer HandlerContextPool_Capacity_DefaultValue() {
            return 1024;
        }
        protected Double HandlerContextPool_PrefillRatio_DefaultValue() {
            return 0.5;
        }
    }
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<HandlerRegistry<? extends HandlerContext<?>>> HandlerRegistry = defineModule(HandlerRegistry.class);
        private Modules() {
            super(HandlerCoordinator.class);
        }
    }

    protected static final Logger logger = LoggerFactory.getLogger(HandlerCoordinator.class);
    protected final HandlerRegistry<HC> handlerRegistry;
    protected final WrapperPool<HR> wrapperPool;
    protected final FixedTaskExecutor<HR, ?> executor;

    protected HandlerCoordinator(NexalithicBuilderContext context, Options options, boolean shared) {
        handlerRegistry = context.getModule(MODULES.HandlerRegistry);
        wrapperPool = new GenericWrapperPool<>(
                PoolStorageFactory.bounded(shared ? MpmcArrayQueue::new : MpscArrayQueue::new, context.getOption(options.HandlerContextPool_Capacity)),
                PoolStrategyFactory.alwaysCreate(),
                this::createRecyclableWrapper
        );
        executor = createFixedTaskExecutor(context, options, shared);
    }
    private FixedTaskExecutor<HR, ?> createFixedTaskExecutor(NexalithicBuilderContext context, Options options, boolean shared) {
        return new FixedTaskExecutor<>(
                context.getOption(options.FixedTaskExecutor.CoreWorkerSize),
                context.getOption(options.FixedTaskExecutor.MaxWorkerSize),
                context.getOption(options.FixedTaskExecutor.KeepAliveTimeMillis),
                BlockingTaskQueue.of(shared
                        ? new MpmcArrayQueue<>(context.getOption(options.FixedTaskExecutor.TaskQueue_Capacity))
                        : new SpmcArrayQueue<>(context.getOption(options.FixedTaskExecutor.TaskQueue_Capacity))
                ),
                new TypedThreadFactory<>() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable runnable) {
                        Thread thread = new Thread(runnable, "HandlerCoordinator-FixedTaskExecutor-" + counter.getAndIncrement());
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                (recyclable, executor) -> recyclable.unwrap().pushResponse(BusinessPacket.create(BusinessPacket.Way.RESPONSE_Busy)),
                (recyclable, thread) -> {
                    NexalithicHandler<HC> handler = recyclable.getHandler();
                    try {
                        handler.handle(recyclable.unwrap());
                    } catch (Exception e) {
                        recyclable.unwrap().pushResponse(BusinessPacket.create(BusinessPacket.Way.RESPONSE_Error));
                    } finally {
                        recyclable.recycle();
                    }
                }
        );
    }

    protected void init(NexalithicBuilderContext context, Options options) {
        wrapperPool.warmUp(context.getOption(options.HandlerContextPool_PrefillRatio));
    }
    protected abstract HR createRecyclableWrapper(GenericWrapperPool<HC, HR> owner);

    public final void accept(S session, BusinessPacket packet) {
        if (packet.getWay().isResponse()) {
            session.getTaskCoordinator().accept(packet);
        } else {
            NexalithicHandler<HC> handler = handlerRegistry.match(packet.getPath());
            if (handler == null) {
                logger.warn("No handler registered for path {}", packet.getPath());
                session.pushBusinessPacket(BusinessPacket.create(BusinessPacket.Way.RESPONSE_NotHandler).setTaskId(packet.getTaskId()));
                return;
            }
            HR recyclable = wrapperPool.acquire();
            if (recyclable == null) {
                logger.warn("No recyclable handler registered for path {}", packet.getPath());
                session.pushBusinessPacket(BusinessPacket.create(BusinessPacket.Way.RESPONSE_Busy).setTaskId(packet.getTaskId()));
                return;
            }
            recyclable.initTarget(packet, session, handler);
            if (!executor.submit(recyclable)) {
                recyclable.recycle();
            }
        }
    }
}
