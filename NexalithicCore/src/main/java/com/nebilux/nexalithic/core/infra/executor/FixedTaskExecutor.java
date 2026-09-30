package com.nebilux.nexalithic.core.infra.executor;

import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 固定任务执行者
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/09
 * @version 1.0.0
 */
public class FixedTaskExecutor<T, TH extends Thread> {
    public static class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> CoreWorkerSize = CoreWorkerSize();
        public final NexalithicOption<Integer> MaxWorkerSize = MaxWorkerSize();
        public final NexalithicOption<Long> KeepAliveTimeMillis = KeepAliveTimeMillis();
        public final NexalithicOption<Integer> TaskQueue_Capacity = TaskQueue_Capacity();
        protected Options(Class<?> holder) {
            super(holder);
        }
        protected NexalithicOption<Integer> CoreWorkerSize() {
            return defineOption(Runtime.getRuntime().availableProcessors(), OptionValidator.nonNegative());
        }
        protected NexalithicOption<Integer> MaxWorkerSize() {
            return defineOption(Runtime.getRuntime().availableProcessors(), OptionValidator.positive());
        }
        protected NexalithicOption<Long> KeepAliveTimeMillis() {
            return defineOption(60_000L, OptionValidator.nonNegative());
        }
        protected NexalithicOption<Integer> TaskQueue_Capacity() {
            return defineOption(1024, OptionValidator.positive());
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(FixedTaskExecutor.class);
    private final int coreWorkerSize;
    private final int maxWorkerSize;
    private final long keepAliveTimeNanos;
    private final BlockingTaskQueue<T> taskQueue;
    private final TypedThreadFactory<TH> threadFactory;
    private final RejectedTaskHandler<T> handler;
    private final TaskProcessor<T, TH> processor;
    private final Set<Worker> workers = ConcurrentHashMap.newKeySet();

    private final AtomicInteger workerCount = new AtomicInteger(0);
    private volatile boolean isShutdown = false;

    public FixedTaskExecutor(int coreWorkerSize, int maxWorkerSize, long keepAliveTimeMillis,
                             BlockingTaskQueue<T> taskQueue, TypedThreadFactory<TH> threadFactory,
                             RejectedTaskHandler<T> handler, TaskProcessor<T, TH> processor) {
        this.coreWorkerSize = coreWorkerSize;
        this.maxWorkerSize = maxWorkerSize;
        this.keepAliveTimeNanos = TimeUnit.MILLISECONDS.toNanos(keepAliveTimeMillis);
        this.taskQueue = taskQueue;
        this.threadFactory = threadFactory;
        this.handler = handler;
        this.processor = processor;
    }

    public boolean submit(T target) {
        if (isShutdown || target == null) {
            return false;
        }
        if (workerCount.get() < coreWorkerSize) {
            if (addWorker(target, true)) {
                return true;
            }
        }
        if (taskQueue.offer(target)) {
            if (workerCount.get() == 0 && !isShutdown) {
                addWorker(null, false);
            }
            return true;
        }
        if (addWorker(target, false)) {
            return true;
        }
        handler.rejectedExecution(target, this);
        return false;
    }

    public void shutdown() {
        isShutdown = true;
        for (Worker worker : workers) {
            worker.stop();
        }
    }

    private boolean addWorker(T firstTask, boolean core) {
        while (true) {
            int wc = workerCount.get();
            int limit = core ? coreWorkerSize : maxWorkerSize;
            if (wc >= limit) {
                return false;
            }
            if (workerCount.compareAndSet(wc, wc + 1)) {
                Worker worker = null;
                try {
                    worker = new Worker(firstTask);
                    workers.add(worker);
                    worker.start();
                    return true;
                } catch (Throwable e) {
                    if (worker != null) {
                        workers.remove(worker);
                    }
                    workerCount.decrementAndGet();
                    throw e;
                }
            }
        }
    }

    private final class Worker implements Runnable {
        private T firstTask;
        private final TH thread;

        Worker(T firstTask) {
            this.firstTask = firstTask;
            thread = threadFactory.newThread(this);
        }

        public void start() {
            thread.start();
        }
        public void stop() {
            thread.interrupt();
        }

        @Override
        public void run() {
            T task = firstTask;
            firstTask = null;
            try {
                while (task != null || (task = getTask()) != null) {
                    try {
                        processor.process(task, thread);
                    } catch (Exception e) {
                        logger.error("Task processing error", e);
                    } finally {
                        task = null;
                    }
                }
            } finally {
                workers.remove(this);
                workerCount.decrementAndGet();
            }
        }

        private T getTask() {
            try {
                return (workerCount.get() > coreWorkerSize) ? taskQueue.poll(keepAliveTimeNanos) : taskQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
    }
}
