package com.nebilux.nexalithic.core.infra.executor;

import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 针对对象 T 的阻塞队列接口
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/09
 * @version 1.0.0
 */
public interface BlockingTaskQueue<T> {

    boolean offer(T task);

    T poll(long timeoutNanos) throws InterruptedException;

    T take() throws InterruptedException;

    void clear();

    static <T> QueueAdapter<T> of(Queue<T> queue) {
        return new QueueAdapter<>(queue);
    }

    /**
     * 将普通 Queue 适配为 BlockingTaskQueue 实现
     */
    final class QueueAdapter<T> implements BlockingTaskQueue<T> {
        private final Queue<T> delegate;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition notEmpty = lock.newCondition();

        public QueueAdapter(Queue<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean offer(T task) {
            if (delegate.offer(task)) {
                final ReentrantLock lock = this.lock;
                lock.lock();
                try {
                    notEmpty.signal();
                } finally {
                    lock.unlock();
                }
                return true;
            }
            return false;
        }

        @Override
        public T poll(long timeoutNanos) throws InterruptedException {
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                T task;
                while ((task = delegate.poll()) == null) {
                    if (timeoutNanos <= 0) {
                        return null;
                    }
                    timeoutNanos = notEmpty.awaitNanos(timeoutNanos);
                }
                return task;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public T take() throws InterruptedException {
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                T task;
                while ((task = delegate.poll()) == null) {
                    notEmpty.await();
                }
                return task;
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void clear() {
            delegate.clear();
        }
    }
}