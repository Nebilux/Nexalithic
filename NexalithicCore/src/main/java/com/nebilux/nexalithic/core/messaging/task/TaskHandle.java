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

import com.nebilux.nexalithic.core.messaging.task.future.TaskFuture;

import java.util.Objects;

/**
 * 任务句柄
 *
 * @author Reonvia
 * @since 0.2.0
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
