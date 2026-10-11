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

package com.nebilux.nexalithic.core.messaging.task;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.executor.BlockingTaskQueue;
import com.nebilux.nexalithic.core.infra.executor.FixedTaskExecutor;
import com.nebilux.nexalithic.core.infra.executor.RejectedTaskHandler;
import com.nebilux.nexalithic.core.infra.executor.TypedThreadFactory;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.infra.timer.TimerContext;
import com.nebilux.nexalithic.core.infra.timer.TimerCoordinator;
import com.nebilux.nexalithic.core.lifecycle.AbstractLifecycle;
import com.nebilux.nexalithic.core.messaging.task.event.TaskEvent;
import com.nebilux.nexalithic.core.messaging.task.event.TaskMailbox;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferListener;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.nebilux.nexalithic.core.exception.FailureSupport.mergePrioritizingError;
import static com.nebilux.nexalithic.core.exception.FailureSupport.unwrapCompletionException;

/**
 * 任务调度程序。构造时仅创建依赖，时间轮由生命周期启动。
 *
 * <p>上层协调器应先排空或中止会话，再关闭本调度器。剩余任务定时器在两种关闭模式下
 * 都执行中止清理，不等待其自然到期；工作线程和监听器执行器会收到关闭请求。</p>
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class TaskScheduler extends AbstractLifecycle implements TimerCoordinator<NexalithicTask> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, TaskScheduler.class);
    public static final class Options extends OptionsDefinition {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {};
        public final FixedTaskExecutor.Options FixedTaskExecutor = new FixedTaskExecutor.Options(holder) {};
        public final NexalithicOption<Long> ProgressPoller_Delay = defineOption(
                500L, OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);
    private final TimeWheel<NexalithicTask> timeWheel;
    private final FixedTaskExecutor<NexalithicTask, ?> taskExecutor;
    private final ScheduledThreadPoolExecutor listenerExecutor;
    private final Set<TransferListener> activeListeners = ConcurrentHashMap.newKeySet();
    private final long progressPollerDelay;
    private final Runnable progressPoller = this::updateListeners;
    private ScheduledFuture<?> progressPollerFuture;

    public TaskScheduler(NexalithicBuilderContext context) {
        super(TaskScheduler.class.getSimpleName());
        progressPollerDelay = context.getOption(OPTIONS.ProgressPoller_Delay);
        timeWheel = initTimeWheel(context);
        taskExecutor = initTaskExecutor(context);
        listenerExecutor = initListenerExecutor();
    }
    private TimeWheel<NexalithicTask> initTimeWheel(NexalithicBuilderContext context) {
        return new TimeWheel<>(
                context.getOption(OPTIONS.TimeWheel.TickMillis),
                context.getOption(OPTIONS.TimeWheel.SlotCount),
                context.getOption(OPTIONS.TimeWheel.TickQuotaShift),
                context.getOption(OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                new GenericWrapperPool<>(
                        PoolStorageFactory.bounded(MpmcArrayQueue::new, context.getOption(OPTIONS.TimeWheel.WrapperPool_Capacity)),
                        PoolStrategyFactory.alwaysCreate(),
                        TimeWheel.ScheduleWrapper<NexalithicTask>::new
                ),
                this,
                TaskScheduler.class.getSimpleName()
        );
    }
    private FixedTaskExecutor<NexalithicTask, ?> initTaskExecutor(NexalithicBuilderContext context) {
        return new FixedTaskExecutor<>(
                context.getOption(OPTIONS.FixedTaskExecutor.CoreWorkerSize),
                context.getOption(OPTIONS.FixedTaskExecutor.MaxWorkerSize),
                context.getOption(OPTIONS.FixedTaskExecutor.KeepAliveTimeMillis),
                BlockingTaskQueue.of(new MpscArrayQueue<>(context.getOption(OPTIONS.FixedTaskExecutor.TaskQueue_Capacity))),
                new TypedThreadFactory<>() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable runnable) {
                        Thread thread = new Thread(runnable, "TaskScheduler-FixedTaskExecutor-" + counter.getAndIncrement());
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                RejectedTaskHandler.discard(),
                (task, thread) -> execute(task)
        );
    }
    private ScheduledThreadPoolExecutor initListenerExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                1,
                runnable -> {
                    Thread thread = new Thread(
                            runnable,
                            "TaskScheduler-TransferListener"
                    );
                    thread.setDaemon(true);
                    return thread;
                }
        );
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        return executor;
    }

    public boolean scheduleNexalithicTask(NexalithicTask task, TaskEvent event) {
        if (task.getState().isTerminal()) {
            return false;
        }
        TaskMailbox mailbox = task.getMailbox();
        return switch (mailbox.offer(event)) {
            case REJECTED -> false;
            case ENQUEUED -> true;
            case ACQUIRED -> {
                if (taskExecutor.submit(task)) {
                    yield true;
                }
                mailbox.close();
                try {
                    executeFailed(task, new RuntimeException("TaskScheduler-FixedTaskExecutor-Rejected"));
                } finally {
                    task.getOwner().getTaskCoordinator().trySubmitWaitingTask(task);
                }
                yield false;
            }
        };
    }

    public void activateNexalithicTask(NexalithicTask task) {
        task.updateLastActiveTime();
        timeWheel.schedule(task);
    }

    public boolean startTransferListener(TransferListener listener, TransferSnapshot snapshot) {
        return executeListenerEvent(() -> {
            if (!activeListeners.add(listener)) {
                return;
            }
            notifyStarted(listener, snapshot);
            if (activeListeners.size() == 1) {
                startProgressPoller();
            }
        });
    }
    public boolean pauseTransferListener(TransferListener listener) {
        return executeListenerEvent(() -> {
            if (activeListeners.contains(listener)) {
                notifyPaused(listener);
            }
        });
    }
    public boolean resumeTransferListener(TransferListener listener) {
        return executeListenerEvent(() -> {
            if (activeListeners.contains(listener)) {
                notifyResumed(listener);
            }
        });
    }
    public boolean finishTransferListener(TransferListener listener) {
        return executeListenerEvent(() -> {
            if (!activeListeners.remove(listener)) {
                return;
            }
            notifyUpdated(listener);
            notifyFinished(listener);
            if (activeListeners.isEmpty()) {
                stopProgressPoller();
            }
        });
    }
    public boolean cancelTransferListener(TransferListener listener) {
        return executeListenerEvent(() -> {
            if (activeListeners.remove(listener) && activeListeners.isEmpty()) {
                stopProgressPoller();
            }
        });
    }

    @Override
    public long getExpiryTimeNanos(TimerContext<NexalithicTask> context) {
        return context.target().getExpiryTimeNanos();
    }

    @Override
    public boolean isCancelled(TimerContext<NexalithicTask> context) {
        return context.target().getFuture().isDone();
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<NexalithicTask> context) {
        NexalithicTask target = context.target();
        if (System.nanoTime() < target.getExpiryTimeNanos()) {
            return false;
        }
        scheduleNexalithicTask(target, TaskEvent.TIMEOUT());
        return true;
    }

    @Override
    protected CompletionStage<Void> doStart() {
        return timeWheel.start();
    }

    @Override
    protected CompletionStage<Void> doShutdown(ShutdownMode mode) {
        // 会话 Loop 先结束；此处不等待剩余任务的定时器自然到期。
        CompletionStage<Void> wheelTermination;
        try {
            wheelTermination = timeWheel.shutdown(ShutdownMode.ABORT);
        } catch (Throwable failure) {
            wheelTermination = CompletableFuture.failedFuture(failure);
        }

        Throwable cleanupFailure = null;
        try {
            taskExecutor.shutdown();
        } catch (Throwable failure) {
            cleanupFailure = mergePrioritizingError(cleanupFailure, failure);
        }
        try {
            listenerExecutor.shutdownNow();
        } catch (Throwable failure) {
            cleanupFailure = mergePrioritizingError(cleanupFailure, failure);
        }
        activeListeners.clear();
        progressPollerFuture = null;

        Throwable finalCleanupFailure = cleanupFailure;
        return wheelTermination.handle((ignored, failure) -> {
            Throwable combined = mergePrioritizingError(finalCleanupFailure, unwrapCompletionException(failure));
            if (combined != null) {
                throw new CompletionException(combined);
            }
            return null;
        });
    }

    private void execute(NexalithicTask task) {
        TaskMailbox mailbox = task.getMailbox();
        do {
            TaskEvent event;
            while ((event = mailbox.poll()) != null) {
                try {
                    switch (event.getType()) {
                        case REQUEST -> executeRequest(task, task.getOwner());
                        case RESPONSE -> executeResponse(task, event.getValue());
                        case COMPLETE -> executeComplete(task);
                        case TIMEOUT -> executeTimeout(task);
                        case FAILURE -> executeFailed(task, event.getValue());
                        case CANCEL -> executeCancel(task);
                    }
                } catch (Exception e) {
                    logger.warn("Exception while executing task {}", task, e);
                } finally {
                    if (task.getState().isTerminal()) {
                        mailbox.close();
                        task.getOwner().getTaskCoordinator().trySubmitWaitingTask(task);
                    }
                }
            }
        } while (mailbox.check());
    }
    private void executeRequest(NexalithicTask task, NexalithicSession<?> session) {
        BusinessPacket request;
        try {
            request = task.request();
            if (request == null) {
                executeFailed(task, new IllegalStateException("Task request returned null"));
                return;
            }
            boolean submitted = session.pushBusinessPacket(request.setTaskId(task.awaitResponse().getTaskId()));
            if (!submitted) {
                executeFailed(task, new IllegalStateException("Task request submission failed"));
            }
        } catch (Exception e) {
            executeFailed(task, e);
            throw e;
        }
    }
    private void executeResponse(NexalithicTask task, BusinessPacket packet) {
        try {
            task.response(packet);
        } catch (Exception e) {
            executeFailed(task, e);
            throw e;
        } finally {
            if (task.getState().isTerminal()) {
                task.getOwner().getTaskCoordinator().remove(task);
            }
        }
    }
    private void executeComplete(NexalithicTask task) {
        try {
            task.complete();
        } finally {
            task.getOwner().getTaskCoordinator().remove(task);
        }
    }
    private void executeTimeout(NexalithicTask task) {
        try {
            task.timeout();
        } finally {
            task.getOwner().getTaskCoordinator().remove(task);
        }
    }
    private void executeCancel(NexalithicTask task) {
        try {
            task.cancel();
        } finally {
            task.getOwner().getTaskCoordinator().remove(task);
        }
    }
    private void executeFailed(NexalithicTask task, Exception exception) {
        try {
            task.failed(exception);
        } finally {
            task.getOwner().getTaskCoordinator().remove(task);
        }
    }

    private boolean executeListenerEvent(Runnable event) {
        try {
            listenerExecutor.execute(event);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }
    private void startProgressPoller() {
        if (progressPollerFuture != null) {
            return;
        }
        progressPollerFuture = listenerExecutor.scheduleWithFixedDelay(
                progressPoller,
                progressPollerDelay,
                progressPollerDelay,
                TimeUnit.MILLISECONDS
        );
    }
    private void stopProgressPoller() {
        ScheduledFuture<?> future = progressPollerFuture;
        progressPollerFuture = null;
        if (future != null) {
            future.cancel(false);
        }
    }
    private void updateListeners() {
        for (TransferListener listener : activeListeners) {
            notifyUpdated(listener);
        }
    }

    private void notifyStarted(TransferListener listener, TransferSnapshot snapshot) {
        try {
            listener.onStarted(snapshot);
        } catch (Exception e) {
            logger.warn("Transfer listener start callback failed", e);
        }
    }
    private void notifyUpdated(TransferListener listener) {
        try {
            listener.onUpdated();
        } catch (Exception e) {
            logger.warn("Transfer listener update callback failed", e);
        }
    }
    private void notifyPaused(TransferListener listener) {
        try {
            listener.onPaused();
        } catch (Exception e) {
            logger.warn("Transfer listener pause callback failed", e);
        }
    }
    private void notifyResumed(TransferListener listener) {
        try {
            listener.onResumed();
        } catch (Exception e) {
            logger.warn("Transfer listener resume callback failed", e);
        }
    }
    private void notifyFinished(TransferListener listener) {
        try {
            listener.onFinished();
        } catch (Exception e) {
            logger.warn("Transfer listener finish callback failed", e);
        }
    }
}
