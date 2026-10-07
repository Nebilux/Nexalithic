package com.nebilux.nexalithic.core.lifecycle;

import com.nebilux.nexalithic.core.infra.concurrent.async.AsyncOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 终端生命周期协调器。
 *
 * <p>协调器管理由多个运行时组件共同构成的终端生命周期。状态只描述终端
 * 当前所处的资源阶段；启动、停止或关闭是否失败，由对应的
 * {@link CompletionStage} 表达。</p>
 *
 * <p>启动、优雅关闭和立即终止操作各自最多触发一次。立即终止可以把正在进行的
 * 优雅关闭从 {@link State#DRAINING} 升级为 {@link State#TERMINATING}，两条终止路径
 * 共享同一个最终完成结果。启动失败时，协调器会使用立即终止流程回滚所有组件，
 * 并在回滚结束后以原始启动失败完成启动阶段；回滚失败会合并到启动失败中。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class LifecycleCoordinator {
    /**
     * 终端生命周期状态。
     *
     * <p>正常启动流程为 {@link #NEW} → {@link #STARTING} →
     * {@link #RUNNING}。立即停止进入 {@link #TERMINATING}；优雅关闭先进入
     * {@link #DRAINING}，随后进入 {@link #TERMINATING}，最终均到达
     * {@link #TERMINATED}。</p>
     */
    public enum State {
        /** 已构建但尚未启动。 */
        NEW,
        /** 正在启动组成终端的运行时组件。 */
        STARTING,
        /** 所有运行时组件均已完成启动。 */
        RUNNING,
        /** 已停止接收新工作，正在排空已经接受的工作。 */
        DRAINING,
        /** 正在立即终止组件或执行最终资源清理。 */
        TERMINATING,
        /** 所有运行时组件均已结束。 */
        TERMINATED;

        /**
         * 判断终端是否已经进入不可逆的终止阶段。
         *
         * @return 当前状态为 {@link #TERMINATING} 或 {@link #TERMINATED} 时返回 {@code true}
         */
        public boolean isTerminationStarted() {
            return this == TERMINATING || this == TERMINATED;
        }
    }

    /** 协调器已经接受的终止意图；具体操作状态由对应的 {@link AsyncOperation} 保存。 */
    private enum TerminationMode {
        /** 尚未接受终止请求。 */
        NONE,
        /** 仅执行优雅关闭。 */
        GRACEFUL,
        /** 仅执行立即终止。 */
        IMMEDIATE,
        /** 优雅关闭期间收到立即终止请求，需要等待两条路径。 */
        ESCALATED
    }

    private static final Logger logger = LoggerFactory.getLogger(LifecycleCoordinator.class);
    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    private final Object lifecycleLock = new Object();
    private final AsyncOperation<Void, Void> startupOperation;
    private final AsyncOperation<Void, Void> shutdownOperation;
    private final AsyncOperation<Void, Void> stopOperation;
    private final AsyncOperation<Void, Void> terminationOperation;
    private final String name;
    /**
     * 受 {@link #lifecycleLock} 保护的终止意图。
     *
     * <p>该值会在释放锁、触发具体 {@link AsyncOperation} 之前发布，用于消除
     * “请求已经被生命周期接受，但操作尚未取得触发权”的短暂窗口。它只描述
     * 路径选择，不重复保存操作是否触发、是否完成或失败原因。</p>
     */
    private TerminationMode terminationMode = TerminationMode.NONE;

    protected LifecycleCoordinator(String name) {
        this.name = Objects.requireNonNull(name, "name");
        startupOperation = new AsyncOperation<>(name + "-startup", this::executeStartup);
        shutdownOperation = new AsyncOperation<>(name + "-shutdown", (ignored, completer) -> executeTerminationAction(this::onShutdown, completer));
        stopOperation = new AsyncOperation<>(name + "-stop", (ignored, completer) -> executeTerminationAction(this::onStop, completer));
        terminationOperation = new AsyncOperation<>(name + "-termination", (ignored, completer) -> {});

        CompletionStage<Throwable> shutdownOutcome = outcomeOf(shutdownOperation.completion());
        CompletionStage<Throwable> stopOutcome = outcomeOf(stopOperation.completion());
        shutdownOutcome.thenAccept(failure -> completeTermination(TerminationMode.GRACEFUL, failure));
        stopOutcome.thenAccept(failure -> completeTermination(TerminationMode.IMMEDIATE, failure));
        stopOutcome.thenCombine(shutdownOutcome, LifecycleCoordinator::mergeFailure)
                .thenAccept(failure -> completeTermination(TerminationMode.ESCALATED, failure));
    }

    /**
     * 启动终端。
     *
     * <p>本方法只负责触发组件启动并立即返回完成阶段。只有所有组件均完成启动后，
     * 终端才会进入 {@link State#RUNNING}，返回阶段才会成功完成。</p>
     *
     * @return 终端启动完成阶段
     * <p>重复调用不会再次执行启动操作，而是记录当前状态并返回首次启动的完成阶段。</p>
     */
    public final CompletionStage<Void> start() {
        State actual = state.compareAndExchange(State.NEW, State.STARTING);
        if (actual != State.NEW) {
            switch (actual) {
                case STARTING -> logger.info("{} startup already in progress", name);
                case RUNNING -> logger.info("{} already running", name);
                case DRAINING -> logger.warn("{} start ignored because shutdown is in progress", name);
                case TERMINATING -> logger.warn("{} start ignored because stop is in progress", name);
                case TERMINATED -> logger.warn("{} start ignored because it is already terminated", name);
                case NEW -> logger.info("{} startup already requested", name);
            }
            return startupOperation.completion();
        }
        logger.info("{} starting", name);
        if (!startupOperation.trigger()) {
            throw new IllegalStateException("Lifecycle startup operation was already triggered for " + name);
        }
        return startupOperation.completion();
    }

    /**
     * 立即终止终端。
     *
     * <p>如果终端正在优雅关闭，本方法会将其升级为立即终止。</p>
     *
     * @return 终端终止完成阶段
     * @throws IllegalStateException 当前状态不允许发起终止
     */
    public final CompletionStage<Void> stop() {
        boolean escalated;
        synchronized (lifecycleLock) {
            State current = state.get();
            if (current.isTerminationStarted()) {
                logger.info("{} stop already requested while in state {}", name, current);
                return terminationOperation.completion();
            }
            if (current != State.RUNNING && current != State.DRAINING) {
                throw invalidTransition("stop", current);
            }
            escalated = current == State.DRAINING;
            state.set(State.TERMINATING);
            terminationMode = escalated ? TerminationMode.ESCALATED : TerminationMode.IMMEDIATE;
        }
        logger.info(escalated ? "{} escalating shutdown to stop" : "{} stopping", name);
        terminationOperation.trigger();
        stopOperation.trigger();
        return terminationOperation.completion();
    }

    /**
     * 优雅关闭终端。
     *
     * @return 终端关闭完成阶段
     * @throws IllegalStateException 当前状态不允许发起关闭
     */
    public final CompletionStage<Void> shutdown() {
        synchronized (lifecycleLock) {
            State current = state.get();
            if (current == State.DRAINING) {
                logger.info("{} shutdown already in progress", name);
                return terminationOperation.completion();
            }
            if (current.isTerminationStarted()) {
                logger.info("{} shutdown ignored because termination is already in state {}", name, current);
                return terminationOperation.completion();
            }
            if (current != State.RUNNING) {
                throw invalidTransition("shutdown", current);
            }
            state.set(State.DRAINING);
            terminationMode = TerminationMode.GRACEFUL;
        }
        logger.info("{} shutting down", name);
        terminationOperation.trigger();
        shutdownOperation.trigger();
        return terminationOperation.completion();
    }

    /**
     * 返回当前终端生命周期状态。
     *
     * @return 当前状态
     */
    public final State getState() {
        return state.get();
    }

    /**
     * 返回终端启动完成阶段。
     *
     * @return 启动完成阶段
     */
    public final CompletionStage<Void> started() {
        return startupOperation.completion();
    }

    /**
     * 返回终端终止完成阶段。
     *
     * @return 终止完成阶段
     */
    public final CompletionStage<Void> termination() {
        return terminationOperation.completion();
    }

    /**
     * 发起全部组件的启动流程。
     *
     * @return 所有组件启动完成阶段
     */
    protected abstract CompletionStage<Void> onStart();

    /**
     * 发起全部组件的立即终止流程。
     *
     * @return 所有组件终止完成阶段
     */
    protected abstract CompletionStage<Void> onStop();

    /**
     * 发起全部组件的优雅关闭流程。
     *
     * @return 所有组件关闭完成阶段
     */
    protected abstract CompletionStage<Void> onShutdown();

    private void executeStartup(Void ignored, AsyncOperation.Completer<Void> completer) {
        CompletionStage<Void> startup = invoke(this::onStart);
        try {
            startup.whenComplete((result, failure) -> {
                Throwable startupFailure = unwrapFailure(failure);
                if (startupFailure == null) {
                    State actual = state.compareAndExchange(State.STARTING, State.RUNNING);
                    if (actual == State.STARTING) {
                        logger.info("{} start succeeded", name);
                        completer.complete(null);
                        return;
                    }
                    startupFailure = new IllegalStateException(
                            "Lifecycle state changed from STARTING to " + actual + " before startup completed"
                    );
                }
                rollbackStartup(startupFailure, completer);
            });
        } catch (Throwable failure) {
            rollbackStartup(failure, completer);
        }
    }

    private void rollbackStartup(Throwable startupFailure, AsyncOperation.Completer<Void> completer) {
        logger.error("{} start failed", name, startupFailure);
        State actual = state.compareAndExchange(State.STARTING, State.TERMINATING);
        if (actual != State.STARTING && !actual.isTerminationStarted()) {
            startupFailure = mergeFailure(startupFailure, new IllegalStateException("Cannot rollback " + name + " startup while in state " + actual));
        }
        logger.info("{} rolling back startup", name);
        try {
            synchronized (lifecycleLock) {
                if (terminationMode == TerminationMode.NONE) {
                    terminationMode = TerminationMode.IMMEDIATE;
                }
            }
            terminationOperation.trigger();
            stopOperation.trigger();
        } catch (Throwable terminationTriggerFailure) {
            startupFailure = mergeFailure(startupFailure, terminationTriggerFailure);
        }
        Throwable recordedFailure = startupFailure;
        try {
            terminationOperation.completion().whenComplete((result, rollbackFailure) ->
                    completer.completeExceptionally(mergeFailure(recordedFailure, unwrapFailure(rollbackFailure)))
            );
        } catch (Throwable completionFailure) {
            completer.completeExceptionally(mergeFailure(recordedFailure, completionFailure));
        }
    }

    private void executeTerminationAction(Supplier<CompletionStage<Void>> action, AsyncOperation.Completer<Void> completer) {
        CompletionStage<Void> termination = invoke(action);
        try {
            termination.whenComplete((result, failure) -> {
                Throwable terminationFailure = unwrapFailure(failure);
                if (terminationFailure == null) {
                    completer.complete(null);
                } else {
                    completer.completeExceptionally(terminationFailure);
                }
            });
        } catch (Throwable failure) {
            completer.completeExceptionally(failure);
        }
    }

    private void completeTermination(TerminationMode completedMode, Throwable failure) {
        synchronized (lifecycleLock) {
            if (terminationMode != completedMode || state.get() == State.TERMINATED) {
                return;
            }
            state.set(State.TERMINATED);
        }
        String operation = switch (completedMode) {
            case GRACEFUL -> "shutdown";
            case IMMEDIATE -> "stop";
            case ESCALATED -> "shutdown escalation";
            case NONE -> throw new IllegalStateException("Cannot complete termination without a termination mode");
        };
        if (failure == null) {
            logger.info("{} {} succeeded", name, operation);
            terminationOperation.completer().complete(null);
        } else {
            logger.error("{} {} failed", name, operation, failure);
            terminationOperation.completer().completeExceptionally(failure);
        }
    }

    private IllegalStateException invalidTransition(String operation, State actual) {
        return new IllegalStateException(
                "Cannot " + operation + " " + name + " while in state " + actual
        );
    }

    private static CompletionStage<Void> invoke(Supplier<CompletionStage<Void>> action) {
        try {
            return Objects.requireNonNull(action.get(), "lifecycle completion stage");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static CompletionStage<Throwable> outcomeOf(CompletionStage<Void> completion) {
        /* 将成功和失败统一成普通结果，使升级路径可以无阻塞地组合两个操作结果。 */
        return completion.handle((ignored, failure) -> unwrapFailure(failure));
    }

    /**
     * 移除异步组合过程中产生的 {@link CompletionException} 包装。
     *
     * @param failure 待规范化的失败
     * @return 原始失败；输入为 {@code null} 时返回 {@code null}
     */
    protected static Throwable unwrapFailure(Throwable failure) {
        Throwable result = failure;
        while (result instanceof CompletionException && result.getCause() != null) {
            result = result.getCause();
        }
        return result;
    }

    /**
     * 合并两个生命周期失败，并优先保留 {@link Error} 作为主失败。
     *
     * @param previous 已记录的失败
     * @param current 新失败
     * @return 合并后的主失败
     */
    protected static Throwable mergeFailure(Throwable previous, Throwable current) {
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
