package com.thezeroer.nexalithic.core.messaging.task;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.executor.BlockingTaskQueue;
import com.thezeroer.nexalithic.core.infra.executor.FixedTaskExecutor;
import com.thezeroer.nexalithic.core.infra.executor.RejectedTaskHandler;
import com.thezeroer.nexalithic.core.infra.executor.TypedThreadFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.thezeroer.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.thezeroer.nexalithic.core.infra.timer.TimeWheel;
import com.thezeroer.nexalithic.core.infra.timer.TimerContext;
import com.thezeroer.nexalithic.core.infra.timer.TimerCoordinator;
import com.thezeroer.nexalithic.core.messaging.task.event.TaskEvent;
import com.thezeroer.nexalithic.core.messaging.task.event.TaskMailbox;
import com.thezeroer.nexalithic.core.messaging.task.visual.TransferListener;
import com.thezeroer.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务调度程序
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/13
 */
public class TaskScheduler implements TimerCoordinator<NexalithicTask> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, TaskScheduler.class);
    public static final class Options extends OptionsDefinition {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {};
        public final FixedTaskExecutor.Options FixedTaskExecutor = new FixedTaskExecutor.Options(holder) {};
        public final NexalithicOption<Long> ProgressPoller_Delay = NexalithicOption.create(
                500L, OptionValidator.positive()
        );
        public Options(Class<?> holder) {
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
        progressPollerDelay = context.getOption(OPTIONS.ProgressPoller_Delay);
        timeWheel = initTimeWheel(context);
        taskExecutor = initTaskExecutor(context);
        listenerExecutor = initListenerExecutor();
    }
    private TimeWheel<NexalithicTask> initTimeWheel(NexalithicBuilderContext context) {
        TimeWheel<NexalithicTask> timeWheel = new TimeWheel<>(
                context.getOption(OPTIONS.TimeWheel.TickMillis),
                context.getOption(OPTIONS.TimeWheel.SlotCount),
                context.getOption(OPTIONS.TimeWheel.TickQuotaShift),
                context.getOption(OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                new GenericWrapperPool<>(
                        PoolStorageFactory.bounded(SpmcArrayQueue::new, context.getOption(OPTIONS.TimeWheel.WrapperPool_Capacity)),
                        PoolStrategyFactory.alwaysCreate(),
                        TimeWheel.ScheduleWrapper<NexalithicTask>::new
                ),
                this,
                TaskScheduler.class.getSimpleName()
        );
        timeWheel.start();
        return timeWheel;
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

    public boolean schedule(NexalithicTask task, TaskEvent event) {
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

    public void activate(NexalithicTask task) {
        task.updateLastActiveTime();
        timeWheel.schedule(task);
    }

    public boolean start(TransferListener listener, TransferSnapshot snapshot) {
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
    public boolean pause(TransferListener listener) {
        return executeListenerEvent(() -> {
            if (activeListeners.contains(listener)) {
                notifyPaused(listener);
            }
        });
    }
    public boolean resume(TransferListener listener) {
        return executeListenerEvent(() -> {
            if (activeListeners.contains(listener)) {
                notifyResumed(listener);
            }
        });
    }
    public boolean finish(TransferListener listener) {
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
    public boolean cancel(TransferListener listener) {
        return executeListenerEvent(() -> {
            if (activeListeners.remove(listener) && activeListeners.isEmpty()) {
                stopProgressPoller();
            }
        });
    }

    public void shutdown() {
        timeWheel.stop();
        taskExecutor.shutdown();
        listenerExecutor.shutdownNow();
        activeListeners.clear();
        progressPollerFuture = null;
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
        schedule(target, TaskEvent.TIMEOUT());
        return true;
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
    private void executeRequest(NexalithicTask task, NexalithicSession<?, ?, ?> session) {
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
        }
    }
    private void executeComplete(NexalithicTask task) {
        task.getOwner().getTaskCoordinator().remove(task);
        task.complete();
    }
    private void executeTimeout(NexalithicTask task) {
        task.getOwner().getTaskCoordinator().remove(task);
        task.timeout();
    }
    private void executeCancel(NexalithicTask task) {
        task.getOwner().getTaskCoordinator().remove(task);
        task.cancel();
    }
    private void executeFailed(NexalithicTask task, Exception exception) {
        task.getOwner().getTaskCoordinator().remove(task);
        task.failed(exception);
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
