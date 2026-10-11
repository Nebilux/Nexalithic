/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.io.loop;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.concurrent.async.AsyncOperation;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalanceable;
import com.nebilux.nexalithic.core.io.thread.LoopThread;
import com.nebilux.nexalithic.core.lifecycle.AbstractLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * 抽象循环。
 *
 * <p>生命周期状态和重复调用由 {@link AbstractLifecycle} 统一管理；Loop 只负责
 * 启动所属线程、封闭工作入口，以及在线程退出时清理资源。关闭钩子返回的
 * {@code loopExited} 只在 {@link #clearLoop()} 完成后结束，避免把“关闭已被接受”
 * 误认为“线程资源已释放”。</p>
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class AbstractLoop extends AbstractLifecycle implements LoadBalanceable {
    public static class Options extends OptionsDefinition {
        public final NexalithicOption<Long> ShutdownTimeoutMillis = defineOption(
                300_000L, OptionValidator.positive()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AbstractLoop.class);
    private record Constant(long ShutdownTimeoutNanos) {}
    private final Constant CONSTANT;
    private final LoopThread thread;
    private final AsyncOperation<Void, Void> sealingOperation;
    private final CompletableFuture<Void> startFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
    private final AtomicBoolean signaled = new AtomicBoolean(false);
    private final AtomicBoolean finalization = new AtomicBoolean(false);
    private final List<Supplier<CompletionStage<Void>>> startStages = new ArrayList<>(4);
    private final List<Supplier<CompletionStage<Void>>> shutdownStages = new ArrayList<>(4);
    private final List<BooleanSupplier> drainConditions = new ArrayList<>(4);
    private final AtomicReference<Throwable> loopFailure = new AtomicReference<>();
    private final CompletableFuture<Long> drainAcceptedAt = new CompletableFuture<>();
    protected final LongAdder loadScore = new LongAdder();

    public AbstractLoop(NexalithicBuilderContext context, Options options) {
        super(clazz -> clazz.getSimpleName() + "-" + context.nextOrdinal(AbstractLoop.class, clazz));
        CONSTANT = context.getConstant(getClass(), Constant.class, () -> new Constant(
                TimeUnit.MILLISECONDS.toNanos(context.getOption(options.ShutdownTimeoutMillis))
        ));
        thread = new LoopThread(context, this::run);
        thread.setDaemon(false);
        thread.setName("LoopThread-" + name);
        sealingOperation = new AsyncOperation<>(name + "-sealing", this::executeSealing);
    }

    public final void wakeup() {
        if (signaled.compareAndSet(false, true)) {
            try {
                wakeupLoop();
            } catch (Throwable throwable) {
                signaled.set(false);
                logger.error("Loop [{}] wakeupLoop() failed", name, throwable);
                throw throwable;
            }
        }
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
        for (BooleanSupplier condition : drainConditions) {
            if (!condition.getAsBoolean()) {
                return false;
            }
        }
        return true;
    }

    public final String getName() {
        return name;
    }
    @Override
    public final long getLoadScore() {
        return loadScore.sum();
    }

    @Override
    protected final CompletionStage<Void> doStart() {
        try {
            CompletionStage<Void> startup = CompletableFuture.completedFuture(null);
            for (Supplier<CompletionStage<Void>> stage : startStages) {
                startup = startup.thenCompose(ignored -> invokeStage(stage));
            }
            return startup.thenCompose(ignored -> {
                try {
                    thread.start();
                    return startFuture;
                } catch (Throwable failure) {
                    return CompletableFuture.failedFuture(failure);
                }
            }).whenComplete((ignored, failure) -> {
                if (failure != null) {
                    recordFailure(unwrapFailure(failure));
                }
            });
        } catch (Throwable failure) {
            recordFailure(failure);
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    protected final void onShutdownAccepted(ShutdownMode mode, State previousState, State nextState, long acceptedAtNanos) {
        if (mode == ShutdownMode.DRAIN && (previousState == State.STARTING || previousState == State.RUNNING)) {
            drainAcceptedAt.complete(acceptedAtNanos);
        } else if (mode == ShutdownMode.ABORT && previousState == State.STARTING) {
            // 基类在启动完成后才运行 doShutdown；立即中止仍需尽早封闭外部入口。
            try {
                seal();
            } catch (Throwable failure) {
                recordFailure(failure);
            }
        }
    }

    @Override
    protected final CompletionStage<Void> doShutdown(ShutdownMode mode) {
        try {
            seal();
        } catch (Throwable throwable) {
            recordFailure(throwable);
        }
        if (thread.getState() == Thread.State.NEW) {
            finishLoop();
        } else {
            wakeup();
        }
        return shutdownFuture;
    }

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

    /** 登记在 Loop 线程启动前依次等待的异步启动阶段。 */
    protected final void registerStartStage(Supplier<CompletionStage<Void>> stage) throws IllegalStateException {
        if (lifecycleState() != State.NEW) {
            throw new IllegalStateException("Start stages can only be registered before the loop starts");
        }
        startStages.add(Objects.requireNonNull(stage, "stage"));
    }

    /** 登记在 Loop 清理后依次等待、完成后才发布终止结果的异步阶段。 */
    protected final void registerShutdownStage(Supplier<CompletionStage<Void>> stage) throws IllegalStateException {
        if (lifecycleState() != State.NEW) {
            throw new IllegalStateException("Shutdown stages can only be registered before the loop starts");
        }
        shutdownStages.add(Objects.requireNonNull(stage, "stage"));
    }

    protected final void registerDrainCondition(BooleanSupplier condition) throws IllegalStateException {
        if (lifecycleState() != State.NEW) {
            throw new IllegalStateException(
                    "isDrainConditions can only be registered before the loop starts"
            );
        }
        drainConditions.add(condition);
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
        try {
            startFuture.complete(null);
            whenStartDecisionMade().toCompletableFuture().join();
            runLoop();
        } catch (Throwable throwable) {
            if (!(throwable instanceof InterruptedException)) {
                recordFailure(throwable);
                logger.error("Loop [{}] terminated unexpectedly", name, throwable);
            }
            startFuture.completeExceptionally(throwable);
        } finally {
            if (lifecycleState() == State.RUNNING || lifecycleState() == State.STARTING) {
                shutdown(ShutdownMode.ABORT);
            }
            finishLoop();
        }
    }
    private void runLoop() throws Exception {
        while (true) {
            State current = lifecycleState();
            switch (current) {
                case RUNNING -> runIteration(false, Long.MAX_VALUE);
                case QUIESCING -> {
                    if (isDrained()) {
                        return;
                    }
                    long remainingNanos = CONSTANT.ShutdownTimeoutNanos() - (System.nanoTime() - drainAcceptedAt.join());
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

    private void finishLoop() {
        if (!finalization.compareAndSet(false, true)) {
            return;
        }
        Throwable terminalFailure = loopFailure.get();
        try {
            seal();
        } catch (Throwable throwable) {
            terminalFailure = mergeThrowable(terminalFailure, throwable);
        }
        try {
            clearLoop();
        } catch (Throwable throwable) {
            terminalFailure = mergeThrowable(terminalFailure, throwable);
        }
        CompletionStage<Throwable> termination = CompletableFuture.completedFuture(terminalFailure);
        for (Supplier<CompletionStage<Void>> stage : shutdownStages) {
            termination = termination.thenCompose(previous -> invokeStage(stage)
                    .handle((ignored, failure) -> mergeThrowable(previous, unwrapFailure(failure))));
        }
        termination.whenComplete((failure, stageFailure) -> {
            Throwable combined = mergeThrowable(failure, unwrapFailure(stageFailure));
            if (combined == null) {
                shutdownFuture.complete(null);
            } else {
                shutdownFuture.completeExceptionally(combined);
            }
        });
    }

    private void seal() throws Throwable {
        boolean initiated;
        try {
            initiated = sealingOperation.trigger();
        } catch (Error fatal) {
            initiated = true;
        }
        Throwable failure = sealingOperation.joinFailure();
        if (failure != null) {
            if (initiated) {
                logger.error("Loop [{}] sealLoop() failed", name, failure);
            }
            throw failure;
        }
    }

    private void executeSealing(Void input, AsyncOperation.Completer<Void> completer) {
        sealLoop();
        completer.complete(null);
    }

    private static CompletionStage<Void> invokeStage(Supplier<CompletionStage<Void>> stage) {
        try {
            return Objects.requireNonNull(stage.get(), "lifecycle callback stage");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private void recordFailure(Throwable failure) {
        loopFailure.accumulateAndGet(failure, AbstractLoop::mergeThrowable);
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
