package com.nebilux.nexalithic.core.infra.recyclable;

import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 通用包装池
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/18
 */
public class GenericWrapperPool<T, W extends GenericWrapperPool.AbstractRecyclableWrapper<T, W>> implements WrapperPool<W> {
    public static class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> StorageCapacity = defineOption(StorageCapacity_DefaultValue(), OptionValidator.positive());
        public final NexalithicOption<Double> PrefillRatio = defineOption(PrefillRatio_DefaultValue(), OptionValidator.closedInterval(0, 1));
        protected Options(Class<?> holder) {
            super(holder);
        }
        protected int StorageCapacity_DefaultValue() {
            return 1024;
        }
        protected double PrefillRatio_DefaultValue() {
            return 0.25;
        }
    }
    private final PoolStorage<W> storage;
    private final PoolStrategy strategy;
    private final WrapperFactory<T, W> wrapperFactory;

    public GenericWrapperPool(PoolStorage<W> storage, PoolStrategy strategy, WrapperFactory<T, W> wrapperFactory) {
        this.storage = storage;
        this.strategy = strategy;
        this.wrapperFactory = wrapperFactory;
    }

    @Override
    public WrapperPool<W> warmUp(double prefillRatio) {
        if (!Double.isFinite(prefillRatio) || prefillRatio < 0.0 || prefillRatio > 1.0) {
            throw new IllegalArgumentException("prefillRatio must be between 0.0 and 1.0");
        }
        int capacity = storage.capacity();
        int size = storage.size();
        if (capacity < 0 || size < 0) {
            throw new IllegalStateException("PoolStorage returned an invalid capacity or size");
        }
        int targetSize = (int) Math.ceil(capacity * prefillRatio);
        int missing = Math.max(0, targetSize - size);
        for (int i = 0; i < missing; i++) {
            W wrapper = wrapperFactory.create(this);
            if (wrapper == null) {
                throw new IllegalStateException("WrapperFactory returned null");
            }
            wrapper.markIdle(this);
            if (!offer(wrapper)) {
                break;
            }
        }
        return this;
    }

    @Override
    public W acquire() {
        PoolStrategy.Permit permit = strategy.beforeAcquire();
        boolean transferred = false;
        try {
            W w = storage.poll();
            if (w == null && strategy.allowCreate()) {
                w = wrapperFactory.create(this);
            }
            if (w != null) {
                w.markActivate(this, permit);
                transferred = true;
            }
            return w;
        } finally {
            if (!transferred) {
                strategy.afterRelease(permit);
            }
        }
    }

    private void release(W wrapper) {
        PoolStrategy.Permit permit = wrapper.detachPermit();
        try {
            try {
                wrapper.onRecycle();
            } catch (Throwable failure) {
                wrapper.markDiscard(failure);
                throw failure;
            }
            wrapper.markIdle();
            offer(wrapper);
        } finally {
            strategy.afterRelease(permit);
        }
    }

    private boolean offer(W wrapper) {
        final boolean offered;
        try {
            offered = storage.offer(wrapper);
        } catch (Throwable failure) {
            wrapper.markDiscard(failure);
            throw failure;
        }
        if (!offered) {
            wrapper.markDiscard(null);
        }
        return offered;
    }

    /**
     * 抽象可回收包装
     *
     * @author tbrtz647@outlook.com
     * @version 1.0.0
     * @since 2026/08/18
     */
    public abstract static class AbstractRecyclableWrapper<T, W extends AbstractRecyclableWrapper<T, W>> implements RecyclableWrapper<T> {
        public enum State {
            NEW,
            ACQUIRING,
            ACTIVE,
            RELEASING,
            IDLE,
            DISCARDED
        }

        private static final VarHandle STATE_VARHANDLE;
        private final GenericWrapperPool<T, W> owner;
        private volatile State state = State.NEW;
        private volatile long stamp;
        private volatile PoolStrategy.Permit permit = PoolStrategy.Permit.NONE;

        static {
            try {
                STATE_VARHANDLE = MethodHandles.lookup().findVarHandle(AbstractRecyclableWrapper.class, "state", State.class);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        public AbstractRecyclableWrapper(GenericWrapperPool<T, W> owner) {
            this.owner = owner;
        }

        @Override
        public final void recycle() {
            if (!transitState(State.ACTIVE, State.RELEASING)) {
                return;
            }
            owner.release(self());
        }

        public final boolean isActive() {
            return state() == State.ACTIVE;
        }

        public final boolean isActive(long expectedStamp) {
            return isActive() && expectedStamp == stamp;
        }

        public final State state() {
            return (State) STATE_VARHANDLE.getVolatile(this);
        }

        public final long stamp() {
            return stamp;
        }

        final void markActivate(GenericWrapperPool<T, W> requester, PoolStrategy.Permit permit) {
            try {
                if (owner != requester) {
                    throw new IllegalStateException("Wrapper belongs to another pool");
                }
                if (!transitState(State.IDLE, State.ACQUIRING) && !transitState(State.NEW, State.ACQUIRING)) {
                    throw new IllegalStateException("Cannot activate wrapper from state: " + state());
                }
                this.permit = permit;
                this.stamp++;
                if (!transitState(State.ACQUIRING, State.ACTIVE)) {
                    this.permit = PoolStrategy.Permit.NONE;
                    throw new IllegalStateException("Failed to activate wrapper");
                }
            } catch (Throwable throwable) {
                markDiscard(throwable);
                throw throwable;
            }
        }

        final void markIdle() {
            if (!transitState(State.RELEASING, State.IDLE)) {
                throw new IllegalStateException("Cannot mark wrapper idle from " + state());
            }
        }

        final void markIdle(GenericWrapperPool<T, W> requester) {
            if (owner != requester) {
                throw new IllegalStateException("Wrapper belongs to another pool");
            }
            if (!transitState(State.NEW, State.IDLE)) {
                throw new IllegalStateException("Cannot initialize idle wrapper from state: " + state());
            }
        }

        final void markDiscard(Throwable cause) {
            setState(State.DISCARDED);
            try {
                onDiscard();
            } catch (Throwable discardFailure) {
                if (cause != null) {
                    cause.addSuppressed(discardFailure);
                } else {
                    throw discardFailure;
                }
            }
        }

        final PoolStrategy.Permit detachPermit() {
            if (state() != State.RELEASING) {
                throw new IllegalStateException("Permit can only be detached while releasing");
            }
            PoolStrategy.Permit current = permit;
            permit = PoolStrategy.Permit.NONE;
            return current;
        }

        protected abstract void onRecycle();
        protected abstract void onReset();
        protected void onDiscard() {}

        private boolean transitState(State expectedState, State newState) {
            return STATE_VARHANDLE.compareAndSet(this, expectedState, newState);
        }
        private void setState(State newState) {
            STATE_VARHANDLE.setVolatile(this, newState);
        }

        @SuppressWarnings("unchecked")
        private W self() {
            return (W) this;
        }
    }

    /**
     * 包装工厂
     *
     * @author tbrtz647@outlook.com
     * @version 1.0.0
     * @since 2026/08/18
     */
    @FunctionalInterface
    public interface WrapperFactory<T, W extends AbstractRecyclableWrapper<T, W>> {
        W create(GenericWrapperPool<T, W> owner);
    }
}
