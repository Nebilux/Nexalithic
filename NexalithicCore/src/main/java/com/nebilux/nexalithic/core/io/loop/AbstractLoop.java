package com.nebilux.nexalithic.core.io.loop;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.concurrent.async.AsyncOperation;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalanceable;
import com.nebilux.nexalithic.core.io.thread.LoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;

/**
 * 抽象循环
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class AbstractLoop implements LoadBalanceable {
    public static class Options extends OptionsDefinition {
        public final NexalithicOption<Long> ShutdownTimeoutMillis = defineOption(
                300_000L, OptionValidator.positive()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
    }

    public enum State {
        /** 已构造，但尚未启动。 */
        NEW,
        /** 执行线程启动中，可能失败 */
        STARTING,
        /** 正常处理事件，并接受新事件。 */
        RUNNING,
        /** 拒绝新事件，继续处理已接受的事件。 */
        DRAINING,
        /** 不再处理事件，正在退出并释放资源。 */
        TERMINATING,
        /** 已退出，所有资源已经释放。 */
        TERMINATED;

        /**
         * 判断循环当前是否仍可接收新的常规工作。
         *
         * <p>{@link #DRAINING} 表示循环已经停止接收新的常规工作，
         * 但仍可能接收完成既有工作所需的维护操作；终止阶段同样不再接收新工作。</p>
         *
         * @return 当前状态为 {@link #NEW}、{@link #STARTING} 或 {@link #RUNNING} 时返回 {@code true}
         */
        public boolean isAcceptingNewWork() {
            return this == NEW || this == STARTING || this == RUNNING;
        }
        public boolean isTerminationStarted() {
            return this == TERMINATING || this == TERMINATED;
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(AbstractLoop.class);
    private record Constant(long ShutdownTimeoutNanos) {}
    private final Constant CONSTANT;
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    private final AtomicBoolean signaled = new AtomicBoolean(false);
    private final LoopThread thread;
    private final Object lifecycleLock = new Object();
    private final AsyncOperation<Void, Void> startupOperation;
    private final AsyncOperation<Void, Void> sealingOperation;
    private final AsyncOperation<Throwable, Void> terminationOperation;
    private final List<BooleanSupplier> isDrainConditions = new ArrayList<>(4);
    private volatile long shutdownStartedNanos;
    protected final String name;
    protected final LongAdder loadScore = new LongAdder();

    public AbstractLoop(NexalithicBuilderContext context, Options options) {
        Class<?> loopType = getClass();
        CONSTANT = context.getConstant(loopType, Constant.class, () -> new Constant(
                TimeUnit.MILLISECONDS.toNanos(context.getOption(options.ShutdownTimeoutMillis))
        ));
        name = loopType.getSimpleName() + "-" + context.nextOrdinal(AbstractLoop.class, loopType);
        thread = new LoopThread(context, this::run);
        thread.setDaemon(false);
        thread.setName("LoopThread-" + name);
        startupOperation = new AsyncOperation<>(name + "-startup", this::executeStartup);
        sealingOperation = new AsyncOperation<>(name + "-sealing", this::executeSealing);
        terminationOperation = new AsyncOperation<>(name + "-termination", this::executeTermination);
    }

    public final CompletionStage<Void> start() throws IllegalStateException {
        synchronized (lifecycleLock) {
            transition(State.NEW, State.STARTING);
            startupOperation.trigger();
        }
        return startupOperation.completion();
    }
    public final CompletionStage<Void> stop() throws IllegalStateException {
        boolean terminateSynchronously;
        synchronized (lifecycleLock) {
            while (true) {
                State current = state.get();
                if (current.isTerminationStarted()) {
                    return terminationOperation.completion();
                }
                if (!state.compareAndSet(current, State.TERMINATING)) {
                    continue;
                }
                terminateSynchronously = current == State.NEW;
                break;
            }
        }
        Throwable failure = null;
        try {
            seal();
        } catch (Throwable throwable) {
            failure = throwable;
        }
        if (terminateSynchronously) {
            terminate(failure);
        } else {
            wakeup();
        }
        return terminationOperation.completion();
    }
    public final CompletionStage<Void> shutdown() throws IllegalStateException {
        boolean terminateSynchronously;
        synchronized (lifecycleLock) {
            while (true) {
                State current = state.get();
                if (current == State.DRAINING || current == State.TERMINATING || current == State.TERMINATED) {
                    return terminationOperation.completion();
                }
                shutdownStartedNanos = System.nanoTime();
                if (!state.compareAndSet(current, current == State.NEW ? State.TERMINATING : State.DRAINING)) {
                    continue;
                }
                terminateSynchronously = current == State.NEW;
                break;
            }
        }
        Throwable failure = null;
        try {
            seal();
        } catch (Throwable throwable) {
            failure = throwable;
        }
        if (terminateSynchronously) {
            terminate(failure);
        } else {
            wakeup();
        }
        return terminationOperation.completion();
    }

    public final void wakeup() {
        if (signaled.compareAndSet(false, true)) {
            try {
                wakeupLoop();
            } catch (Throwable failure) {
                signaled.set(false);
                logger.error("Loop [{}] wakeupLoop() failed", name, failure);
            }
        }
    }

    public final CompletionStage<Void> started() {
        return startupOperation.completion();
    }

    public final CompletionStage<Void> termination() {
        return terminationOperation.completion();
    }

    public final LoopBuffer aquireLoopBuffer() {
        return thread.aquireLoopBuffer();
    }

    /**
     * 判断当前线程是否为该 Loop 的所属线程。
     */
    public final boolean inEventLoop() {
        return Thread.currentThread() == thread;
    }
    /**
     * 已接受的事件、待写数据及其他异步操作是否均已完成。
     */
    public final boolean isDrained() {
        for (BooleanSupplier condition : isDrainConditions) {
            if (!condition.getAsBoolean()) {
                return false;
            }
        }
        return true;
    }

    public final String getName() {
        return name;
    }
    public final State getState() {
        return state.get();
    }

    @Override
    public long getLoadScore() {
        return loadScore.sum();
    }

    protected void initLoop() {}
    protected abstract void wakeupLoop();
    /**
     * 封闭当前 Loop 的外部工作入口。
     * <p>该方法最多执行一次。</p>
     * <p>该方法可能由任意线程调用。子类必须通过
     * {@link #inEventLoop()} 判断是否可以直接操作 Loop 所属资源。</p>
     *
     * <p>非 Loop 线程不得直接修改 SelectionKey、Selector 状态或
     * 其他仅归 Loop 线程所有的数据；可以设置线程安全标志，
     * 实际操作由后续 runIteration 完成。</p>
     */
    protected abstract void sealLoop();
    /**
     * 释放当前 Loop 持有的全部资源。
     * <p>该方法最多执行一次。</p>
     * <ul>
     *     <li>若 {@link #inEventLoop()} 为 true，则由所属 Loop 线程清理。</li>
     *     <li>若为 false，则表示 LoopThread 尚未成功启动，不存在并发的所有者线程。</li>
     * </ul>
     *
     * <p>该方法必须同步完成清理；返回后终止操作将被完成。</p>
     */
    protected abstract void clearLoop();
    /**
     * 执行一次完整且有界的 Loop 迭代。
     *
     * <p>实现必须在有限时间内返回，不得执行无限队列排空、
     * 无期限阻塞 I/O、Thread.sleep 或不可控的用户回调。</p>
     *
     * @param draining 当前是否正在优雅关闭
     * @param maxWaitNanos 本次迭代最多允许用于阻塞等待的时间，非 draining 状态下可忽略
     */
    protected abstract void runIteration(boolean draining, long maxWaitNanos) throws Exception;

    protected final void isDrainCondition(BooleanSupplier condition) throws IllegalStateException {
        if (getState() != State.NEW) {
            throw new IllegalStateException(
                    "isDrainConditions can only be registered before the loop starts"
            );
        }
        isDrainConditions.add(condition);
    }
    protected final boolean acknowledgeWakeup() {
        return signaled.getAndSet(false);
    }
    protected final boolean isSealed() {
        return sealingOperation.isTriggered();
    }

    private void run() {
        if (!inEventLoop()) {
            throw new IllegalStateException("Loop must be executed by its owner thread");
        }
        Throwable failure = null;
        try {
            synchronized (lifecycleLock) {
                State current = state.get();
                if (current.isTerminationStarted()) {
                    return;
                }
                if (current != State.STARTING && current != State.DRAINING) {
                    throw new IllegalStateException(
                            "Loop [%s] cannot initialize in state %s"
                                    .formatted(name, current)
                    );
                }
            }
            initLoop();
            synchronized (lifecycleLock) {
                State current = state.get();
                switch (current) {
                    case STARTING -> transition(State.STARTING, State.RUNNING);
                    case DRAINING -> {}
                    case TERMINATING, TERMINATED -> {
                        return;
                    }
                    default -> throw new IllegalStateException(
                            "Loop [%s] initialized in unexpected state %s"
                                    .formatted(name, current)
                    );
                }
            }
            startupOperation.completer().complete(null);
            runLoop();
        } catch (Throwable throwable) {
            if (!(throwable instanceof InterruptedException)) {
                failure = throwable;
                logger.error("Loop [{}] terminated unexpectedly", name, throwable);
            }
            startupOperation.completer().completeExceptionally(throwable);
        } finally {
            terminate(failure);
        }
    }
    private void runLoop() throws Exception {
        while (true) {
            State current = state.get();
            switch (current) {
                case RUNNING -> runIteration(false, Long.MAX_VALUE);
                case DRAINING -> {
                    if (isDrained()) {
                        return;
                    }
                    long remainingNanos = CONSTANT.ShutdownTimeoutNanos() - (System.nanoTime() - shutdownStartedNanos);
                    if (remainingNanos <= 0) {
                        return;
                    }
                    runIteration(true, remainingNanos);
                }
                case TERMINATING, TERMINATED -> {
                    return;
                }
                default -> throw new IllegalStateException(
                        "Loop [%s] entered processing loop in state %s"
                                .formatted(name, current)
                );
            }
        }
    }

    private void executeStartup(Void input, AsyncOperation.Completer<Void> completer) {
        try {
            thread.start();
        } catch (Throwable failure) {
            terminate(failure);
            throw failure;
        }
    }
    private void executeSealing(Void input, AsyncOperation.Completer<Void> completer) {
        sealLoop();
        completer.complete(null);
    }
    private void executeTermination(Throwable input, AsyncOperation.Completer<Void> completer) {
        Throwable terminalFailure = input;
        try {
            seal();
        } catch (Throwable throwable) {
            terminalFailure = mergeThrowable(terminalFailure, throwable);
        }
        try {
            clearLoop();
        } catch (Throwable throwable) {
            terminalFailure = mergeThrowable(terminalFailure, throwable);
        } finally {
            state.set(State.TERMINATED);
        }
        if (!startupOperation.isDone()) {
            Throwable startupFailure = terminalFailure;
            if (startupFailure == null) {
                startupFailure = new CancellationException("Loop [" + name + "] terminated before startup completed");
            }
            if (!startupOperation.abort(startupFailure)) {
                startupOperation.completer().completeExceptionally(startupFailure);
            }
        }
        if (terminalFailure == null) {
            completer.complete(null);
        } else {
            completer.completeExceptionally(terminalFailure);
            if (terminalFailure instanceof Error error) {
                throw error;
            }
        }
    }

    private void seal() throws Throwable {
        boolean initiated;
        try {
            initiated = sealingOperation.trigger();
        } catch (Error fatal) {
            initiated = true;
        }
        Throwable failure = null;
        try {
            sealingOperation.completion().toCompletableFuture().join();
        } catch (CompletionException exception) {
            failure = exception.getCause();
        } catch (Exception exception) {
            failure = exception;
        }
        if (failure != null) {
            if (initiated) {
                logger.error("Loop [{}] sealLoop() failed", name, failure);
            }
            throw failure;
        }
    }

    private void terminate(Throwable failure) {
        while (true) {
            State current = state.get();
            if (current == State.TERMINATED) {
                return;
            }
            if (current == State.TERMINATING || state.compareAndSet(current,State.TERMINATING)) {
                break;
            }
        }
        terminationOperation.trigger(failure);
    }

    private void transition(State expected, State targeted) {
        State actual = state.compareAndExchange(expected, targeted);
        if (actual != expected) {
            throw new IllegalStateException(
                    "Loop [%s] state transition failed: expected=%s, actual=%s, targeted=%s"
                            .formatted(name, expected, actual, targeted)
            );
        }
    }

    protected static Throwable mergeThrowable(Throwable previous, Throwable current) {
        if (previous == null) {
            return current;
        }
        if (current == null || previous == current) {
            return previous;
        }
        if (!(previous instanceof Error) && current instanceof Error) {
            current.addSuppressed(previous);
            return current;
        }
        previous.addSuppressed(current);
        return previous;
    }
}
