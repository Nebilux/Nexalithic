package com.nebilux.nexalithic.core.infra.async;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 可由任意线程触发，并由任意线程提供最终结果的一次性异步操作。
 *
 * <p>首个成功调用 {@link #trigger()} 的线程会同步执行 {@link Action}。
 * Action 正常返回只表示发起或交接阶段成功，不表示整个异步操作已经完成。
 * 最终结果由 Action 或其他线程通过 {@link Completer} 提供。</p>
 *
 * <p>{@link #completion()} 只有在 Action 正常返回且最终结果已经提供后才会成功完成。
 * Action 或最终结果任一失败，都会使组合结果异常完成。</p>
 *
 * @param <T> 异步操作的最终结果类型
 * @author Reonvia
 * @since 0.2.2
 */
public final class AsyncOperation<T> {
    /**
     * 异步操作的受限完成器。
     *
     * <p>完成器可以被传递给其他线程，使其能够提供操作的最终结果，
     * 但不暴露触发操作或访问其他内部状态的能力。</p>
     *
     * @param <T> 异步操作的最终结果类型
     * @author Reonvia
     * @since 0.2.2
     */
    public interface Completer<T> {
        /**
         * 尝试以成功结果完成操作。
         *
         * @param result 操作结果，可以为 {@code null}
         * @return 当前调用是否首次完成结果
         */
        boolean complete(T result);

        /**
         * 尝试以异常完成操作。
         *
         * @param failure 操作失败原因
         * @return 当前调用是否首次完成结果
         */
        boolean completeExceptionally(Throwable failure);
    }
    /**
     * 异步操作的发起动作。
     *
     * <p>Action 在成功取得触发权的线程中同步执行。
     * Action 可以立即完成结果，也可以将 {@link Completer} 传递给其他线程。
     * Action 正常返回只表示发起阶段成功。</p>
     *
     * @param <T> 异步操作的最终结果类型
     * @author Reonvia
     * @since 0.2.2
     */
    @FunctionalInterface
    public interface Action<T> {
        /**
         * 执行操作的发起或交接逻辑。
         *
         * @param completer 可供当前线程或其他线程提供最终结果的完成器
         * @throws Exception 发起或交接操作失败
         */
        void execute(Completer<T> completer) throws Exception;
    }
    /** 用于日志、诊断和区分不同异步操作的名称。 */
    private final String name;
    /** 首次触发操作时执行的发起动作。 */
    private final Action<T> action;
    /** 保证操作最多只有一个触发线程。 */
    private final AtomicBoolean triggered = new AtomicBoolean(false);
    /** Action 阶段的完成结果。 */
    private final CompletableFuture<Void> actionFuture = new CompletableFuture<>();
    /** 由 {@link Completer} 提供的最终操作结果。 */
    private final CompletableFuture<T> resultFuture = new CompletableFuture<>();
    /** Action 阶段和最终结果都成功后完成的组合结果。 */
    private final CompletableFuture<T> finalFuture = actionFuture.thenCompose(ignored -> resultFuture);
    /** 仅向 Action 及其派生的异步工作暴露结果完成能力。 */
    private final Completer<T> completer = new Completer<>() {
        @Override
        public boolean complete(T result) {
            return AsyncOperation.this.resultFuture.complete(result);
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            return AsyncOperation.this.resultFuture.completeExceptionally(failure);
        }
    };

    /**
     * 创建异步操作。
     *
     * @param name 操作名称
     * @param action 首次触发时执行的发起动作
     * @throws NullPointerException {@code name} 或 {@code action} 为 {@code null}
     */
    public AsyncOperation(String name, Action<T> action) {
        this.name = Objects.requireNonNull(name, "name");
        this.action = Objects.requireNonNull(action, "action");
    }

    /**
     * 尝试触发异步操作。
     *
     * <p>首个成功更新触发标记的线程会同步执行 Action；
     * 后续调用不会重复执行 Action。</p>
     *
     * @return 当前调用是否成功取得唯一触发权
     */
    public boolean trigger() {
        if (!triggered.compareAndSet(false, true)) {
            return false;
        }
        execute();
        return true;
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
     */
    public boolean abort(Throwable failure) {
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
    public Completer<T> completer() {
        return completer;
    }

    /**
     * @return 操作的唯一触发权是否已被触发或中止操作占用
     */
    public boolean isTriggered() {
        return triggered.get();
    }
    /**
     * @return 最终组合结果是否已完成
     */
    public boolean isDone() {
        return finalFuture.isDone();
    }
    /**
     * @return 最终组合结果是否以异常完成
     */
    public boolean isCompletedExceptionally() {
        return finalFuture.isCompletedExceptionally();
    }
    /**
     * @return 操作已触发，但 Action 尚未返回或抛出异常
     */
    public boolean isExecutingAction() {
        return isTriggered() && !actionFuture.isDone();
    }
    /**
     * @return Action 已正常返回，但最终结果尚未提供
     */
    public boolean isAwaitingResult() {
        return actionFuture.isDone() && !actionFuture.isCompletedExceptionally() && !resultFuture.isDone();
    }

    /**
     * 获取只读的最终完成阶段。
     *
     * <p>返回的阶段不暴露内部 {@link CompletableFuture} 的完成和取消能力。</p>
     *
     * @return Action 和异步结果组后的只读阶段
     */
    public CompletionStage<T> completion() {
        return finalFuture.minimalCompletionStage();
    }

    /**
     * @return 异步操作名称
     */
    public String getName() {
        return name;
    }

    private void execute() {
        try {
            action.execute(completer);
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
