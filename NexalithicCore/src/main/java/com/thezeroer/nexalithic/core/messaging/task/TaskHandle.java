package com.thezeroer.nexalithic.core.messaging.task;

import com.thezeroer.nexalithic.core.messaging.task.future.TaskFuture;

import java.util.Objects;

/**
 * 任务句柄
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/15
 */
public final class TaskHandle {
    private final TaskFuture future;

    TaskHandle(TaskFuture future) {
        this.future = Objects.requireNonNull(future, "future");
    }

    public NexalithicTask.State getState() {
        return future.getState();
    }

    public boolean cancel() {
        return future.cancel();
    }

    public void waitFinish() {
        future.waitFinish();
    }

    public boolean waitFinish(long timeout) {
        return future.waitFinish(timeout);
    }

    public boolean isDone() {
        return future.isDone();
    }
}
