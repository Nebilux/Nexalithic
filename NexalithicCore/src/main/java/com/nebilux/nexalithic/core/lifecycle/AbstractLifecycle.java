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

package com.nebilux.nexalithic.core.lifecycle;

import com.nebilux.nexalithic.core.infra.concurrent.async.AsyncOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 单个组件的一次性生命周期控制，实现 {@link Lifecycle} 的状态转换和完成阶段管理。
 *
 * <p>启动、排空关闭和中止关闭各自最多触发一次；重复调用共享相应的最终结果。
 * 在 {@link State#STARTING} 收到关闭请求时，仅记录关闭模式，待启动钩子结束后
 * 才执行关闭钩子。启动钩子失败时改走中止清理；成功但已收到关闭请求时，
 * 启动阶段最终以取消异常完成，并等待关闭流程结束后才发布该结果。</p>
 *
 * <p>运行中请求排空会进入 {@link State#QUIESCING}。随后请求中止时，
 * 如果排空操作尚未触发，则只运行中止钩子；否则进入升级路径，
 * 两个关闭钩子可能并行，终止结果等待两者都完成。中止钩子的实现须使
 * 已开始的排空钩子最终结束，否则升级路径的终止阶段也无法完成。</p>
 *
 * <p>对尚未启动的组件请求关闭会改走中止路径，且不会执行启动钩子。
 * 当前实现会先使 {@link #whenStarted()} 异常完成，再触发中止清理；
 * 非异步完成回调可能在调用 {@link #shutdown(ShutdownMode)} 的线程中执行。
 * 因此，启动失败回调不得同步等待 {@link #whenTerminated()}，否则可能阻止
 * 中止清理被触发。这是当前执行顺序的限制，而非终止已完成的保证。</p>
 *
 * <p>状态只描述进度，不表示操作成功；应检查 {@link #whenStarted()} 和
 * {@link #whenTerminated()} 的结果。钩子返回的阶段必须在实际工作结束后才完成，
 * 不得返回依赖自身完成的生命周期阶段。</p>
 *
 * @author Reonvia
 * @since 0.3.1
 */
public abstract class AbstractLifecycle implements Lifecycle {
    /** 内部关闭路径；ESCALATED 表示排空已触发后又请求中止。 */
    private enum TerminationMode {
        NONE,
        DRAIN,
        ABORT,
        ESCALATED
    }
    private static final Logger logger = LoggerFactory.getLogger(AbstractLifecycle.class);
    /** 状态更新受 lifecycleLock 保护；读取可不持锁。 */
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    /** 串行化状态转换和关闭模式选择，不覆盖钩子的实际执行时间。 */
    private final Object lifecycleLock = new Object();
    /** 启动与两种关闭操作分别占有一次性触发权。 */
    private final AsyncOperation<Void, Void> startOperation;
    private final AsyncOperation<Void, Void> drainOperation;
    private final AsyncOperation<Void, Void> abortOperation;
    /** 由当前最终关闭路径完成；升级时须等待排空与中止两个结果。 */
    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
    private final String name;
    /** 仅在 lifecycleLock 内读写；关闭请求可以将 DRAIN 升级为 ABORT 或 ESCALATED。 */
    private TerminationMode terminationMode = TerminationMode.NONE;

    /**
     * 建立三个一次性操作以及终止结果的组合关系，不会执行任何生命周期钩子。
     *
     * @param name 用于标识组件及其异步操作的名称，不可为 {@code null}
     */
    public AbstractLifecycle(String name) {
        this.name = Objects.requireNonNull(name, "name");
        startOperation = new AsyncOperation<>(name + "-start", this::executeStart);
        drainOperation = AsyncOperation.fromStage(name + "-drain", ignored -> executeShutdown(ShutdownMode.DRAIN));
        abortOperation = AsyncOperation.fromStage(name + "-abort", ignored -> executeShutdown(ShutdownMode.ABORT));

        // 将钩子的异常转为正常完成的 Throwable 值，使升级路径即使遇到失败也能等待两端。
        CompletionStage<Throwable> drainOutcome = outcomeOf(drainOperation.completion());
        CompletionStage<Throwable> abortOutcome = outcomeOf(abortOperation.completion());
        drainOutcome.thenAccept(failure -> finishTermination(TerminationMode.DRAIN, failure));
        abortOutcome.thenAccept(failure -> finishTermination(TerminationMode.ABORT, failure));
        drainOutcome.thenCombine(abortOutcome, AbstractLifecycle::mergeFailure)
                .thenAccept(failure -> finishTermination(TerminationMode.ESCALATED, failure));
    }

    /**
     * 仅从 NEW 转入 STARTING 并触发一次启动；其他状态直接返回已有启动结果。
     *
     * <p>触发动作可能在调用线程中同步执行；返回的阶段才表示启动的最终结果。</p>
     *
     * @return 启动完成阶段
     */
    @Override
    public final CompletionStage<Void> start() {
        State currentState;
        synchronized (lifecycleLock) {
            currentState = state.get();
            if (currentState == State.NEW) {
                state.set(State.STARTING);
            }
        }
        if (currentState != State.NEW) {
            if (currentState == State.STARTING) {
                logger.info("{} startup already in progress", name);
            } else if (currentState == State.RUNNING) {
                logger.info("{} already running", name);
            } else {
                logger.warn("{} start ignored in state {}", name, currentState);
            }
            return whenStarted();
        }
        logger.info("{} startup requested", name);
        startOperation.trigger();
        return whenStarted();
    }

    /**
     * 请求关闭并返回共享的终止完成阶段。
     *
     * <p>NEW 状态总是走 ABORT，即使请求的是 DRAIN；STARTING 状态只记录请求，
     * 待启动钩子结束后再触发关闭。QUIESCING 状态下的 ABORT 可能升级为并行清理。
     * 关闭钩子和非异步完成回调都可能在当前调用线程中执行，故本方法的调用本身
     * 不保证立即返回。</p>
     *
     * @param mode 请求的关闭策略，不可为 {@code null}
     * @return 终止完成阶段；关闭失败时异常完成
     */
    @Override
    public final CompletionStage<Void> shutdown(ShutdownMode mode) {
        Objects.requireNonNull(mode, "mode");
        TerminationMode toTrigger;
        TerminationMode previousMode;
        State currentState;
        State nextState;
        boolean noOp = false;
        synchronized (lifecycleLock) {
            currentState = state.get();
            previousMode = terminationMode;
            switch (currentState) {
                case NEW -> {
                    terminationMode = TerminationMode.ABORT;
                    state.set(State.TERMINATING);
                }
                case STARTING -> {
                    if (mode == ShutdownMode.ABORT) {
                        terminationMode = TerminationMode.ABORT;
                    } else if (terminationMode == TerminationMode.NONE) {
                        terminationMode = TerminationMode.DRAIN;
                    }
                }
                case RUNNING -> {
                    terminationMode = mode == ShutdownMode.DRAIN ? TerminationMode.DRAIN : TerminationMode.ABORT;
                    state.set(mode == ShutdownMode.DRAIN ? State.QUIESCING : State.TERMINATING);
                }
                case QUIESCING -> {
                    if (mode == ShutdownMode.DRAIN) {
                        noOp = true;
                    } else {
                        terminationMode = drainOperation.isTriggered() ? TerminationMode.ESCALATED : TerminationMode.ABORT;
                        state.set(State.TERMINATING);
                    }
                }
                case TERMINATING, TERMINATED -> noOp = true;
                default -> throw new IllegalStateException("Unexpected Lifecycle.State: " + currentState);
            }
            toTrigger = terminationMode;
            nextState = state.get();
        }
        if (previousMode != toTrigger || currentState != nextState) {
            logger.info("{} shutdown {} requested: {} -> {} (mode {} -> {})",
                    name, mode, currentState, nextState, previousMode, toTrigger);
        } else {
            logger.info("{} shutdown {} request unchanged in state {} (mode {})",
                    name, mode, currentState, toTrigger);
        }
        if (noOp) {
            return whenTerminated();
        }
        if (currentState == State.NEW) {
            // 当前顺序会先发布启动取消，再触发下面的 ABORT。若启动完成回调在本线程
            // 同步等待 whenTerminated()，该回调会阻塞 triggerTermination 的执行。
            startOperation.abort(new CancellationException(name + " was shutdown before start"));
        }
        if (currentState != State.STARTING) {
            triggerTermination(toTrigger);
        }
        return whenTerminated();
    }

    /** {@inheritDoc} */
    @Override
    public final CompletionStage<Void> whenStarted() {
        return startOperation.completion();
    }

    /** {@inheritDoc} */
    @Override
    public final CompletionStage<Void> whenTerminated() {
        return terminationFuture.minimalCompletionStage();
    }

    /** {@inheritDoc} */
    @Override
    public final State lifecycleState() {
        return state.get();
    }

    /**
     * 返回构造时指定的组件名称。
     *
     * @return 组件名称
     */
    public final String name() {
        return name;
    }

    /**
     * 执行组件启动，并返回覆盖全部启动工作的完成阶段。
     *
     * <p>不得返回 {@link #whenStarted()}，否则自身完成将依赖自身。
     * 启动失败或启动期间收到关闭请求后，本类会决定后续清理路径。</p>
     *
     * @return 实际启动工作的完成阶段，不可为 {@code null}
     */
    protected abstract CompletionStage<Void> doStart();

    /**
     * 执行指定关闭模式的清理，并返回覆盖全部清理工作的完成阶段。
     *
     * <p>ABORT 可能在 doStart 从未执行、启动仅部分完成或 DRAIN 正在执行时被调用。
     * 升级时两种模式可能并行；ABORT 实现应促使正在进行的 DRAIN 最终完成。
     * 不得返回 {@link #whenTerminated()}，否则终止结果将依赖自身。</p>
     *
     * @param mode 实际执行的关闭模式
     * @return 实际清理工作的完成阶段，不可为 {@code null}
     */
    protected abstract CompletionStage<Void> doShutdown(ShutdownMode mode);

    /** 启动操作取得触发权后检查是否已有关闭请求，并接入 doStart 的完成结果。 */
    private void executeStart(Void ignored, AsyncOperation.Completer<Void> completer) {
        boolean cancelled;
        synchronized (lifecycleLock) {
            cancelled = terminationMode != TerminationMode.NONE;
        }
        if (cancelled) {
            finishStart(completer, new CancellationException(name + " was shutdown during start"));
            return;
        }
        invoke(this::doStart).whenComplete((result, failure) -> finishStart(completer, unwrapFailure(failure)));
    }

    /**
     * 根据启动钩子的结果和已记录的关闭请求，确定下一状态与关闭路径。
     *
     * <p>启动失败时强制选择 ABORT；启动成功但已请求关闭时先转入
     * QUIESCING 或 TERMINATING，再触发相应操作，允许并发的 ABORT 请求
     * 正确识别和升级排空。此处只在终止完成后才发布未成功启动的结果。</p>
     */
    private void finishStart(AsyncOperation.Completer<Void> completer, Throwable throwable) {
        TerminationMode toTrigger;
        boolean started;
        boolean shutdownRequested;
        synchronized (lifecycleLock) {
            shutdownRequested = terminationMode != TerminationMode.NONE;
            started = throwable == null && this.terminationMode == TerminationMode.NONE;
            if (started) {
                state.set(State.RUNNING);
            } else if (throwable != null) {
                terminationMode = TerminationMode.ABORT;
                state.set(State.TERMINATING);
            } else {
                state.set(terminationMode == TerminationMode.DRAIN ? State.QUIESCING : State.TERMINATING);
            }
            toTrigger = terminationMode;
        }
        if (started) {
            logger.info("{} startup completed", name);
            completer.complete(null);
            return;
        }
        if (throwable == null) {
            logger.info("{} startup completed with pending {} shutdown", name, toTrigger);
        } else if (shutdownRequested && throwable instanceof CancellationException) {
            logger.info("{} startup cancelled by shutdown request", name);
        } else {
            logger.error("{} startup failed; aborting", name, throwable);
        }
        // 启动失败或启动中关闭时，保留原始原因，并把清理失败合并进启动结果。
        terminationFuture.whenComplete((result, terminationFailure) ->
                completer.completeExceptionally(mergeFailure(
                        throwable == null ? new CancellationException(name + " was shutdown before start completed") : throwable,
                        unwrapFailure(terminationFailure)))
        );
        triggerTermination(toTrigger);
    }

    /**
     * 执行关闭钩子；若尚未触发的 DRAIN 已被 ABORT 取代，则跳过过期的排空动作。
     * 已触发的 DRAIN 在 ESCALATED 模式下仍需完成，以便汇总两条关闭路径。
     */
    private CompletionStage<Void> executeShutdown(ShutdownMode mode) {
        if (mode == ShutdownMode.DRAIN) {
            synchronized (lifecycleLock) {
                if (terminationMode == TerminationMode.ABORT) {
                    return CompletableFuture.completedFuture(null);
                }
            }
        }
        logger.info("{} {} shutdown started", name, mode);
        return invoke(() -> doShutdown(mode));
    }

    /** 根据已选择的模式触发一次性关闭操作；升级时 DRAIN 已由先前请求触发。 */
    private void triggerTermination(TerminationMode mode) {
        switch (mode) {
            case DRAIN -> drainOperation.trigger();
            case ABORT, ESCALATED -> abortOperation.trigger();
            case NONE -> {}
            default -> throw new IllegalArgumentException("Unexpected Termination.Mode: " + mode);
        }
    }

    /**
     * 只接受当前模式对应的最终结果。升级后单个钩子的结果不会提前结束生命周期。
     * TERMINATED 表示关闭流程结束，即使其完成阶段携带失败。
     */
    private void finishTermination(TerminationMode mode, Throwable throwable) {
        synchronized (lifecycleLock) {
            if (terminationMode != mode || state.get() == State.TERMINATED) {
                return;
            }
            state.set(State.TERMINATED);
        }
        if (throwable == null) {
            logger.info("{} terminated after {} shutdown", name, mode);
            terminationFuture.complete(null);
        } else {
            logger.error("{} termination failed after {} shutdown", name, mode, throwable);
            terminationFuture.completeExceptionally(throwable);
        }
    }

    /** 将钩子的同步抛错或空返回值统一转成失败的完成阶段。 */
    private static CompletionStage<Void> invoke(Supplier<CompletionStage<Void>> action) {
        try {
            return Objects.requireNonNull(action.get(), "lifecycle completion stage");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** 把完成阶段的失败转换为正常完成的 Throwable 值，供两条关闭路径合并。 */
    private static CompletionStage<Throwable> outcomeOf(CompletionStage<Void> completion) {
        return completion.handle((result, failure) -> unwrapFailure(failure));
    }

    /** 去掉异步组合产生的 CompletionException 包装，保留底层失败原因。 */
    private static Throwable unwrapFailure(Throwable failure) {
        Throwable result = failure;
        while (result instanceof CompletionException && result.getCause() != null) {
            result = result.getCause();
        }
        return result;
    }

    /** 合并失败；优先保留 Error，另一原因记录为 suppressed exception。 */
    private static Throwable mergeFailure(Throwable previous, Throwable current) {
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
