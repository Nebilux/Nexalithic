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

package com.nebilux.nexalithic.core.infra.executor;

import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 针对对象 T 的阻塞队列接口
 *
 * @author Reonvia
 * @since 0.1.0
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