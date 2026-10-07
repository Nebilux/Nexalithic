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

package com.nebilux.nexalithic.core.messaging.task.event;

import org.jctools.queues.MpscUnboundedArrayQueue;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 任务邮箱
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class TaskMailbox {
    public enum OfferResult {
        REJECTED,   // Mailbox 已关闭
        ENQUEUED,   // 已有 Worker
        ACQUIRED    // 调用方需要提交 Worker
    }
    private enum State {
        CLOSED,
        IDLE,
        PROCESSING,
    }
    private final Queue<TaskEvent> events = new MpscUnboundedArrayQueue<>(4);
    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);

    public OfferResult offer(TaskEvent event) {
        synchronized (this) {
            if (state.get() == State.CLOSED) {
                return OfferResult.REJECTED;
            }
            events.offer(event);
            return state.compareAndSet(State.IDLE, State.PROCESSING) ? OfferResult.ACQUIRED : OfferResult.ENQUEUED;
        }
    }
    public TaskEvent poll() {
        if (state.get() == State.CLOSED) {
            return null;
        }
        return events.poll();
    }

    public boolean check() {
        if (!state.compareAndSet(State.PROCESSING, State.IDLE)) {
            return false;
        }
        if (events.isEmpty()) {
            return false;
        }
        return state.compareAndSet(State.IDLE, State.PROCESSING);
    }

    public void close() {
        synchronized (this) {
            state.set(State.CLOSED);
            events.clear();
        }
    }
}
