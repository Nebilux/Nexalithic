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

import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 任务事件
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class TaskEvent {
    public enum Type {
        REQUEST,
        RESPONSE,
        COMPLETE,
        TIMEOUT,
        CANCEL,
        FAILURE,
    }
    private static final TaskEvent REQUEST = new TaskEvent(Type.REQUEST);
    private static final TaskEvent COMPLETE = new TaskEvent(Type.COMPLETE);
    private static final TaskEvent TIMEOUT = new TaskEvent(Type.TIMEOUT);
    private static final TaskEvent CANCEL = new TaskEvent(Type.CANCEL);
    private final Type type;
    private final Object value;

    private TaskEvent(Type type, Object value) {
        this.type = type;
        this.value = value;
    }
    private TaskEvent(Type type) {
        this(type, null);
    }

    public static TaskEvent REQUEST() {
        return REQUEST;
    }
    public static TaskEvent RESPONSE(BusinessPacket packet) {
        return new TaskEvent(Type.RESPONSE, packet);
    }
    public static TaskEvent COMPLETE() {
        return COMPLETE;
    }
    public static TaskEvent TIMEOUT() {
        return TIMEOUT;
    }
    public static TaskEvent FAILURE(Exception exception) {
        return new TaskEvent(Type.FAILURE, exception);
    }
    public static TaskEvent CANCEL() {
        return CANCEL;
    }

    public Type getType() {
        return type;
    }
    @SuppressWarnings("unchecked")
    public <T> T getValue() {
        return (T) value;
    }
}
