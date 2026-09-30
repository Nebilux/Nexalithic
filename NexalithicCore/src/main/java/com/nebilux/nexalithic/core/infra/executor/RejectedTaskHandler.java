package com.nebilux.nexalithic.core.infra.executor;

/**
 * 当队列满且无法增加线程时，如何处理对象 T
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/09
 * @version 1.0.0
 */
@FunctionalInterface
public interface RejectedTaskHandler<T> {
    void rejectedExecution(T task, FixedTaskExecutor<T, ?> executor);

    /**
     * 丢弃任务，不抛出异常。适用于非关键性的后台任务。
     */
    static <T> RejectedTaskHandler<T> discard() {
        return new DiscardPolicy<>();
    }

    /**
     * 由提交任务的线程直接执行该任务。
     * 优点：提供反馈压（Back-pressure），减缓提交速度。
     * 警告：如果提交线程是 IO 线程（如 EventLoop），可能会导致 IO 阻塞。
     */
    static <T> RejectedTaskHandler<T> callerRuns(TaskProcessor<T, ?> processor) {
        return new CallerRunsPolicy<>(processor);
    }

    /**
     * 直接抛出异常。
     */
    static <T> RejectedTaskHandler<T> abort() {
        return new AbortPolicy<>();
    }

    /**
     * 静默丢弃策略
     */
    final class DiscardPolicy<T> implements RejectedTaskHandler<T> {
        @Override
        public void rejectedExecution(T task, FixedTaskExecutor<T, ?> executor) {
        }
    }

    /**
     * 提交者运行策略
     */
    final class CallerRunsPolicy<T> implements RejectedTaskHandler<T> {
        private final TaskProcessor<T, ?> processor;

        public CallerRunsPolicy(TaskProcessor<T, ?> processor) {
            this.processor = processor;
        }

        @Override
        public void rejectedExecution(T task, FixedTaskExecutor<T, ?> executor) {
            if (processor != null) {
                processor.process(task, null);
            }
        }
    }

    /**
     * 中止并抛出异常策略
     */
    final class AbortPolicy<T> implements RejectedTaskHandler<T> {
        @Override
        public void rejectedExecution(T task, FixedTaskExecutor<T, ?> executor) {
            throw new RuntimeException("Task " + task + " rejected from " + executor);
        }
    }
}