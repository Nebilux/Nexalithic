package com.thezeroer.nexalithic.core.infra.recyclable;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

/**
 * 池存储工厂。
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/19
 */
public final class PoolStorageFactory {
    private PoolStorageFactory() {}

    /**
     * 创建由 {@link ConcurrentLinkedQueue} 支撑的严格有界存储。
     *
     * @param capacity 最大缓存数量
     * @param <E>      元素类型
     * @return 池存储
     */
    public static <E> PoolStorage<E> bounded(int capacity) {
        return bounded(new ConcurrentLinkedQueue<>(), capacity);
    }

    /**
     * 通过队列工厂创建严格有界存储。
     *
     * @param factory 队列工厂，入参为期望容量
     * @param capacity     最大缓存数量
     * @param <E>          元素类型
     * @return 池存储
     */
    public static <E> PoolStorage<E> bounded(IntFunction<? extends Queue<E>> factory, int capacity) {
        requireCapacity(capacity);
        Queue<E> queue = factory.apply(capacity);
        return bounded(queue, capacity);
    }

    /**
     * 将队列包装为严格有界存储。
     * <p>
     * 创建完成后，底层队列只能由返回的 {@link PoolStorage} 访问；外部直接修改
     * 队列会破坏内部数量统计。底层队列原有的并发访问约束不会被此包装器改变。
     *
     * @param queue    底层队列
     * @param capacity 最大缓存数量
     * @param <E>      元素类型
     * @return 池存储
     */
    public static <E> PoolStorage<E> bounded(Queue<E> queue, int capacity) {
        return new BoundedQueueStorage<>(queue, capacity);
    }

    private static void requireCapacity(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity must not be negative");
        }
    }

    /**
     * 使用独立计数器对任意队列施加严格容量限制。
     */
    private static final class BoundedQueueStorage<E> implements PoolStorage<E> {
        private final Queue<E> queue;
        private final int capacity;
        private final AtomicInteger size;

        private BoundedQueueStorage(Queue<E> queue, int capacity) {
            this.queue = Objects.requireNonNull(queue, "queue");
            requireCapacity(capacity);
            if (!queue.isEmpty()) {
                throw new IllegalArgumentException("Pool storage queue must be empty");
            }
            this.capacity = capacity;
            this.size = new AtomicInteger();
        }

        @Override
        public boolean offer(E value) {
            while (true) {
                int current = size.get();
                if (current >= capacity) {
                    return false;
                }
                if (size.compareAndSet(current, current + 1)) {
                    break;
                }
            }

            boolean offered = false;
            try {
                offered = queue.offer(value);
                return offered;
            } finally {
                if (!offered) {
                    size.decrementAndGet();
                }
            }
        }

        @Override
        public E poll() {
            E value = queue.poll();
            if (value == null) {
                return null;
            }
            int remaining = size.decrementAndGet();
            if (remaining < 0) {
                size.incrementAndGet();
                throw new IllegalStateException("Underlying queue was modified externally");
            }
            return value;
        }

        @Override
        public int capacity() {
            return capacity;
        }

        @Override
        public int size() {
            return size.get();
        }
    }
}
