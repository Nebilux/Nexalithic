package com.thezeroer.nexalithic.core.messaging.task.event;

import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 任务事件
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/17
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
