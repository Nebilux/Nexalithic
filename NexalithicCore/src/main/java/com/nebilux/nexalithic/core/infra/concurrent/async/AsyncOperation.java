package com.nebilux.nexalithic.core.infra.concurrent.async;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 接受一次触发输入，并由任意线程提供最终结果的一次性异步操作。
 *
 * <p>{@link #trigger(Object)} 与 {@link #abort(Throwable)} 竞争同一个一次性触发权。
 * 首个成功调用 {@code trigger} 的线程会把输入传给 {@link Action}，并在当前线程中同步执行它；
 * 后续触发或中止调用均不会再次执行 Action。</p>
 *
 * <p>Action 正常返回只表示发起或交接阶段已经完成，不代表整个异步操作完成。
 * 最终结果可以由 Action 立即提供，也可以由持有 {@link Completer} 的其他线程稍后提供。
 * 即使结果先于 Action 返回，{@link #completion()} 也不会在 Action 阶段结束前完成。</p>
 *
 * <p>Action 抛出的 {@link Exception} 和 Completer 提供的失败都会使组合结果异常完成；
 * Action 抛出的 {@link Error} 会先记录到组合结果，再重新抛给触发线程。</p>
 *
 * @param <I> 触发操作时传递给 Action 的输入类型
 * @param <R> 异步操作的最终结果类型
 * @author Reonvia
 * @since 0.2.2
 */
public final class AsyncOperation<I, R> {
    /**
     * 异步操作的发起动作。
     *
     * <p>Action 在成功取得触发权的线程中同步执行。
     * Action 可以立即完成结果，也可以将 {@link Completer} 传递给其他线程；
     * 正常返回只表示发起或交接阶段成功。</p>
     *
     * @param <I> 触发输入类型
     * @param <R> 异步操作的最终结果类型
     * @author Reonvia
     * @since 0.2.2
     */
    @FunctionalInterface
    public interface Action<I, R> {
        /**
         * 执行操作的发起或交接逻辑。
         *
         * @param input 首次成功触发操作时提供的输入，可以为 {@code null}
         * @param completer 可供当前线程或其他线程提供最终结果的完成器
         * @throws Exception 发起或交接操作失败
         */
        void execute(I input, Completer<R> completer) throws Exception;
    }
    /**
     * 异步操作的受限完成器。
     *
     * <p>完成器可以被传递给其他线程，使其能够提供操作的最终结果，
     * 但不暴露触发操作或访问其他内部状态的能力。成功或失败结果只能设置一次，
     * 后续完成调用返回 {@code false}。</p>
     *
     * @param <R> 异步操作的最终结果类型
     * @author Reonvia
     * @since 0.2.2
     */
    public interface Completer<R> {
        /**
         * 尝试以成功结果完成操作。
         *
         * @param result 操作结果，可以为 {@code null}
         * @return 当前调用是否首次完成结果
         */
        boolean complete(R result);

        /**
         * 尝试以异常完成操作。
         *
         * @param failure 操作失败原因
         * @return 当前调用是否首次完成结果
         * @throws NullPointerException {@code failure} 为 {@code null}
         */
        boolean completeExceptionally(Throwable failure);
    }
    /** 用于日志、诊断和区分不同异步操作的名称。 */
    private final String name;
    /** 首次触发操作时执行的发起动作。 */
    private final Action<I, R> action;
    /** 保证 {@code trigger} 和 {@code abort} 中最多只有一个调用取得触发权。 */
    private final AtomicBoolean triggered = new AtomicBoolean(false);
    /** 记录 Action 正常返回或异常退出的完成阶段。 */
    private final CompletableFuture<Void> actionFuture = new CompletableFuture<>();
    /** 由 {@link Completer} 提供的最终操作结果。 */
    private final CompletableFuture<R> resultFuture = new CompletableFuture<>();
    /** 顺序组合 Action 阶段与结果阶段，对外表示整个操作的最终状态。 */
    private final CompletableFuture<R> finalFuture = actionFuture.thenCompose(ignored -> resultFuture);
    /** 仅向 Action 及其派生的异步工作暴露结果完成能力。 */
    private final Completer<R> completer = new Completer<>() {
        @Override
        public boolean complete(R result) {
            return AsyncOperation.this.resultFuture.complete(result);
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return AsyncOperation.this.resultFuture.completeExceptionally(
                    Objects.requireNonNull(failure, "failure")
            );
        }
    };

    /**
     * 创建异步操作。
     *
     * @param name 操作名称
     * @param action 首次触发时执行的发起动作
     * @throws NullPointerException {@code name} 或 {@code action} 为 {@code null}
     */
    public AsyncOperation(String name, Action<I, R> action) {
        this.name = Objects.requireNonNull(name, "name");
        this.action = Objects.requireNonNull(action, "action");
    }

    /**
     * 尝试触发异步操作。
     *
     * <p>首个成功占用触发权的线程会把 {@code input} 传给 Action，
     * 并在当前线程中同步执行 Action。后续触发或中止调用不会重复执行 Action。</p>
     *
     * <p>返回 {@code true} 只表示当前调用取得了触发权，不表示 Action 或整个操作成功。
     *
     * @param input 传递给 Action 的触发输入，可以为 {@code null}
     * @return 当前调用是否取得唯一触发权；若操作已触发或中止则返回 {@code false}
     * @throws Error Action 抛出的严重错误
     */
    public boolean trigger(I input) {
        if (!triggered.compareAndSet(false, true)) {
            return false;
        }
        execute(input);
        return true;
    }

    /**
     * 使用 {@code null} 输入尝试触发异步操作。
     *
     * <p>该方法等价于 {@code trigger(null)}，适用于输入类型为 {@link Void}
     * 或 Action 明确接受 {@code null} 的操作。</p>
     *
     * @return 当前调用是否取得唯一触发权；若操作已触发或中止则返回 {@code false}
     * @throws Error Action 抛出的严重错误
     */
    public boolean trigger() {
        return trigger(null);
    }

    /**
     * 在 Action 尚未触发时中止操作。
     *
     * <p>成功中止会原子占用唯一触发权，因此 Action 之后不会再执行，
     * Action 阶段和最终结果都会以指定失败原因完成。若操作已经触发，
     * 本方法不会干预正在执行或等待结果的操作。</p>
     *
     * @param failure 中止原因
     * @return 当前调用是否在 Action 触发前成功中止操作
     * @throws NullPointerException {@code failure} 为 {@code null}
     */
    public boolean abort(Throwable failure) {
        Objects.requireNonNull(failure, "failure");
        if (!triggered.compareAndSet(false, true)) {
            return false;
        }
        failAction(failure);
        return true;
    }

    /**
     * 获取可传递给其他线程的受限完成器。
     *
     * @return 当前操作的完成器
     */
    public Completer<R> completer() {
        return completer;
    }

    /**
     * 判断一次性触发权是否已经被占用。
     *
     * @return 操作的唯一触发权是否已被 {@code trigger} 或 {@code abort} 占用
     */
    public boolean isTriggered() {
        return triggered.get();
    }
    /**
     * 判断整个操作是否已经完成。
     *
     * @return Action 阶段与最终结果组成的完成阶段是否已经结束
     */
    public boolean isDone() {
        return finalFuture.isDone();
    }
    /**
     * 判断整个操作是否异常完成。
     *
     * @return 组合完成阶段是否因 Action 失败、结果失败或中止而异常完成
     */
    public boolean isCompletedExceptionally() {
        return finalFuture.isCompletedExceptionally();
    }
    /**
     * 判断 Action 当前是否仍在执行。
     *
     * @return 操作已由 {@code trigger} 触发，但 Action 尚未返回或抛出异常
     */
    public boolean isExecutingAction() {
        return isTriggered() && !actionFuture.isDone();
    }
    /**
     * 判断操作是否正在等待最终结果。
     *
     * @return Action 已正常返回，但 Completer 尚未提供最终结果
     */
    public boolean isAwaitingResult() {
        return actionFuture.isDone() && !actionFuture.isCompletedExceptionally() && !resultFuture.isDone();
    }

    /**
     * 获取只读的最终完成阶段。
     *
     * <p>返回的阶段不暴露内部 {@link CompletableFuture} 的完成和取消能力。
     * 只有 Action 正常返回且 Completer 成功提供结果时，该阶段才会成功完成。</p>
     *
     * @return Action 阶段和最终结果组合后的只读阶段
     */
    public CompletionStage<R> completion() {
        return finalFuture.minimalCompletionStage();
    }

    /**
     * 获取用于日志和诊断的操作名称。
     *
     * @return 异步操作名称
     */
    public String getName() {
        return name;
    }

    private void execute(I input) {
        try {
            action.execute(input, completer);
            actionFuture.complete(null);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            failAction(interruptedException);
        } catch (Exception exception) {
            failAction(exception);
        } catch (Error error) {
            failAction(error);
            throw error;
        }
    }
    private void failAction(Throwable failure) {
        actionFuture.completeExceptionally(failure);
        resultFuture.completeExceptionally(failure);
    }
}
