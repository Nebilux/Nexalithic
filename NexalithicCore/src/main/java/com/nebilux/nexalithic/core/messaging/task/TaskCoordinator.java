package com.nebilux.nexalithic.core.messaging.task;

import com.nebilux.nexalithic.core.messaging.task.event.TaskEvent;
import com.nebilux.nexalithic.core.messaging.task.future.TaskFuture;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 任务协调员
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/12
 */
public class TaskCoordinator {
    private final NexalithicSession<?> owner;
    private final TaskScheduler scheduler;
    private final ConcurrentMap<Long, NexalithicTask> activeTasks = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<NexalithicTask> waitingTasks = new ConcurrentLinkedQueue<>();
    private final AtomicReference<NexalithicTask> currentSequentialTask = new AtomicReference<>();

    public TaskCoordinator(NexalithicSession<?> owner, TaskScheduler scheduler) {
        this.owner = owner;
        this.scheduler = scheduler;
    }

    public TaskHandle submit(NexalithicTask.Builder taskBuilder) {
        NexalithicTask task = taskBuilder.build(owner);
        track(task);
        TaskFuture future = task.getFuture();
        if (!switch (task.getStrategy()) {
            case IMMEDIATE -> scheduler.schedule(task, TaskEvent.REQUEST());
            case SEQUENTIAL -> {
                synchronized (this) {
                    if (!currentSequentialTask.compareAndSet(null, task)) {
                        yield waitingTasks.offer(task.awaitRequest());
                    }
                }
                yield scheduler.schedule(task, TaskEvent.REQUEST());
            }
        }) {
            untrack(task);
        }
        return new TaskHandle(future);
    }

    public void accept(BusinessPacket packet) {
        NexalithicTask activeTask = pick(packet.getTaskId());
        if (activeTask == null) {
            return;
        }
        activeTask.updateLastActiveTime();
        scheduler.schedule(activeTask, TaskEvent.RESPONSE(packet));
    }

    public void activate(NexalithicTask task) {
        if (task.getPattern() == NexalithicTask.Pattern.ONE_WAY) {
            untrack(task);
            scheduler.schedule(task, TaskEvent.COMPLETE());
            return;
        }
        scheduler.activate(task);
    }

    public void remove(NexalithicTask task) {
        activeTasks.remove(task.getTaskId(), task);
        waitingTasks.remove(task);
    }

    public NexalithicTask find(long taskId) {
        return activeTasks.get(taskId);
    }

    public TaskScheduler getScheduler() {
        return scheduler;
    }

    public void trySubmitWaitingTask(NexalithicTask activeTask) {
        NexalithicTask waitingTask;
        synchronized (this) {
            if (currentSequentialTask.get() != activeTask) {
                return;
            }
            waitingTask = waitingTasks.poll();
            currentSequentialTask.set(waitingTask);
        }
        if (waitingTask == null) {
            return;
        }
        scheduler.schedule(waitingTask, TaskEvent.REQUEST());
    }

    private void track(NexalithicTask task) {
        if (task.getOwner() != owner) {
            throw new IllegalArgumentException("Target Session Mismatch");
        }
        if (activeTasks.putIfAbsent(task.getTaskId(), task) != null) {
            throw new RuntimeException("Task " + task.getTaskId() + " already registered");
        }
    }
    private void untrack(NexalithicTask task) {
        if (task.getOwner() != owner) {
            throw new IllegalArgumentException("Target Session Mismatch");
        }
        activeTasks.remove(task.getTaskId(), task);
    }
    private NexalithicTask pick(long taskId) {
        NexalithicTask task = activeTasks.get(taskId);
        if (task == null) {
            return null;
        }
        if (task.getPattern() == NexalithicTask.Pattern.STREAM) {
            return task;
        }
        return activeTasks.remove(taskId, task) ? task : null;
    }
}
