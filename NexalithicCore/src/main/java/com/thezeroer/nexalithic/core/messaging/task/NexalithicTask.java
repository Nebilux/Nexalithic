package com.thezeroer.nexalithic.core.messaging.task;

import com.thezeroer.nexalithic.core.messaging.handler.NexalithicHandler;
import com.thezeroer.nexalithic.core.messaging.task.event.TaskMailbox;
import com.thezeroer.nexalithic.core.messaging.task.future.TaskFuture;
import com.thezeroer.nexalithic.core.messaging.task.visual.TransferListener;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.session.NexalithicSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <h1>Nexalithic 异步任务 (Task)</h1>
 * <p>该类代表一次主动发起的通信请求交互，通常遵循 “请求-响应” 模式。</p>
 * <p><b>核心特性：</b></p>
 * <ul>
 * <li><b>关联性：</b> 通过 {@code taskId} 追踪并匹配对端返回的回执包。</li>
 * <li><b>状态化：</b> 支持超时控制、重试机制以及异步结果生成（Future/Promise）。</li>
 * <li><b>双工支持：</b> 客户端和服务端均可发起任务以驱动对端执行特定逻辑。</li>
 * </ul>
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/15
 * @see NexalithicHandler
 */
public class NexalithicTask {
    public enum Pattern {
        /** 只有请求，无回执。发送完即销毁。 */
        ONE_WAY,

        /** 标准请求-响应。收到一个回执或超时后销毁。 */
        REQUEST_RESPONSE,

        /** 流模式。持续接收回执，直到手动取消。 */
        STREAM
    }
    /**
     * 任务提交策略。
     * 默认为{@link Strategy#IMMEDIATE}
     */
    public enum Strategy {
        IMMEDIATE,
        SEQUENTIAL
    }
    public enum State {
        NEW(Phase.CREATED),
        ENQUEUED(Phase.QUEUED),

        REQUESTING(Phase.REQUEST),
        SENDING(Phase.REQUEST),

        WAITING(Phase.WAITING),
        STREAMING(Phase.WAITING),

        RESPONDING(Phase.RESPONSE),

        COMPLETED(Phase.TERMINAL),
        TIMEOUT(Phase.TERMINAL),
        FAILED(Phase.TERMINAL),
        CANCELLED(Phase.TERMINAL);

        private final Phase phase;

        State(Phase phase) {
            this.phase = phase;
        }

        public Phase phase() {
            return phase;
        }

        public boolean isTerminal() {
            return phase == Phase.TERMINAL;
        }

        public enum Phase {
            CREATED,
            QUEUED,
            REQUEST,
            WAITING,
            RESPONSE,
            TERMINAL
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(NexalithicTask.class);
    private static final AtomicLong COUNTER = new AtomicLong(0);
    private final long taskId;
    private final AtomicReference<State> state = new  AtomicReference<>(State.NEW);
    private final TaskFunction.RequestAction requestAction;
    private final TaskFunction.ResponseAction responseAction;
    private final TaskFunction.CompleteAction completeAction;
    private final TaskFunction.TimeoutAction timeoutAction;
    private final TaskFunction.FailedAction failedAction;
    private final TaskFunction.CancelAction cancelAction;
    private final TaskFunction.FinishAction finishAction;
    private final TransferListener requestListener;
    private final TransferListener responseListener;
    private final Pattern pattern;
    private final Strategy strategy;
    private final long waitTimeNanos;

    private final TaskFuture future;
    private final TaskMailbox mailbox;
    private final NexalithicSession<?> owner;
    private volatile long lastActiveTimeNanos = -1;

    private NexalithicTask(TaskFunction.RequestAction requestAction, TaskFunction.ResponseAction responseAction, TaskFunction.CompleteAction completeAction,
                           TaskFunction.TimeoutAction timeoutAction, TaskFunction.FailedAction failedAction, TaskFunction.CancelAction cancelAction,
                           TaskFunction.FinishAction finishAction, TransferListener requestListener, TransferListener responseListener,
                           Pattern pattern, Strategy strategy, long waitTimeNanos, NexalithicSession<?> owner) {
        this.taskId = COUNTER.getAndIncrement();
        this.requestAction = requestAction;
        this.responseAction = responseAction;
        this.completeAction = completeAction;
        this.timeoutAction = timeoutAction;
        this.failedAction = failedAction;
        this.cancelAction = cancelAction;
        this.finishAction = finishAction;
        this.requestListener = requestListener;
        this.responseListener = responseListener;
        this.pattern = pattern;
        this.strategy = strategy;
        this.waitTimeNanos = waitTimeNanos;
        this.owner = owner;
        future = new TaskFuture(this);
        mailbox = new TaskMailbox();
    }

    public static Builder builder() {
        return new Builder();
    }

    public BusinessPacket request() {
        if (state.get() == State.NEW || state.get() == State.ENQUEUED) {
            state.set(State.REQUESTING);
        } else {
            throw new IllegalStateException("State " + state.get() + " is not in NEW or ENQUEUED state");
        }
        return requestAction.execute();
    }
    public void response(BusinessPacket packet) {
        if (pattern == Pattern.REQUEST_RESPONSE) {
            if (state.compareAndSet(State.WAITING, State.RESPONDING)) {
                responseAction.execute(packet, future);
                complete();
            }
        } else {
            if (state.get() == State.STREAMING || state.compareAndSet(State.WAITING, State.STREAMING)) {
                responseAction.execute(packet, future);
            }
        }
    }
    public void complete() {
        if (state.get().isTerminal()) {
            return;
        }
        state.set(State.COMPLETED);
        try {
            completeAction.execute();
        } finally {
            finish();
        }
    }
    public void timeout() {
        if (state.get().isTerminal()) {
            return;
        }
        state.set(State.TIMEOUT);
        try {
            timeoutAction.execute();
        } finally {
            finish();
        }
    }
    public void cancel() {
        if (state.get().isTerminal()) {
            return;
        }
        state.set(State.CANCELLED);
        try {
            cancelAction.execute();
        } finally {
            finish();
        }
    }
    public void failed(Exception exception) {
        if (state.get().isTerminal()) {
            return;
        }
        state.set(State.FAILED);
        try {
            failedAction.execute(exception);
        } finally {
            finish();
        }
    }
    public void finish() {
        try {
            finishAction.execute();
        } finally {
            future.internalComplete();
        }
    }

    public TransferListener getRequestListener() {
        return requestListener;
    }
    public TransferListener getResponseListener() {
        return responseListener;
    }

    public NexalithicSession<?> getOwner() {
        return owner;
    }
    public TaskFuture getFuture() {
        return future;
    }
    public TaskMailbox getMailbox() {
        return mailbox;
    }

    public long getTaskId() {
        return taskId;
    }
    public Pattern getPattern() {
        return pattern;
    }
    public Strategy getStrategy() {
        return strategy;
    }
    public State getState() {
        return state.get();
    }
    public long getExpiryTimeNanos() {
        return lastActiveTimeNanos + waitTimeNanos;
    }

    void updateLastActiveTime() {
        lastActiveTimeNanos = System.nanoTime();
    }

    NexalithicTask awaitRequest() {
        if (!state.compareAndSet(State.NEW, State.ENQUEUED)) {
            throw new IllegalStateException("State " + state.get() + " is not in NEW state");
        }
        return this;
    }
    NexalithicTask awaitResponse() {
        if (pattern == Pattern.ONE_WAY) {
            return this;
        }
        if (!state.compareAndSet(State.REQUESTING, State.WAITING)) {
            throw new IllegalStateException("State " + state.get() + " is not in REQUESTING state");
        }
        return this;
    }

    public static class Builder {
        private TaskFunction.RequestAction requestAction;
        private TaskFunction.ResponseAction responseAction = (packet, future) -> {};
        private TaskFunction.CompleteAction completeAction = () -> {};
        private TaskFunction.TimeoutAction timeoutAction = () -> {};
        private TaskFunction.FailedAction failedAction = exception -> {
            if (exception != null) {
                logger.warn("Exception in NexalithicTask", exception);
            }
        };
        private TaskFunction.CancelAction cancelAction = () -> {};
        private TaskFunction.FinishAction finishAction = () -> {};
        private TransferListener requestListener;
        private TransferListener responseListener;
        private Pattern pattern = Pattern.REQUEST_RESPONSE;
        private Strategy strategy = Strategy.IMMEDIATE;
        private long waitTimeNanos = TimeUnit.SECONDS.toNanos(3);

        public Builder onRequest(TaskFunction.RequestAction requestAction) {
            this.requestAction = requestAction;
            return this;
        }
        public Builder onRequest(BusinessPacket packet) {
            this.requestAction = () -> packet;
            return this;
        }
        public Builder onResponse(TaskFunction.ResponseAction responseAction) {
            this.responseAction = responseAction;
            return this;
        }
        public Builder onResponse(TaskFunction.SimpleResponseAction responseAction) {
            this.responseAction = (packet, future) -> responseAction.execute(packet);
            return this;
        }
        public Builder onComplete(TaskFunction.CompleteAction completeAction) {
            this.completeAction = completeAction;
            return this;
        }
        public Builder onTimeout(TaskFunction.TimeoutAction timeoutAction) {
            this.timeoutAction = timeoutAction;
            return this;
        }
        public Builder onFailed(TaskFunction.FailedAction failedAction) {
            this.failedAction = failedAction;
            return this;
        }
        public Builder onCancel(TaskFunction.CancelAction cancelAction) {
            this.cancelAction = cancelAction;
            return this;
        }
        public Builder onFinish(TaskFunction.FinishAction finishAction) {
            this.finishAction = finishAction;
            return this;
        }

        public Builder requestListener(TransferListener requestListener) {
            this.requestListener = requestListener;
            return this;
        }
        public Builder responseListener(TransferListener responseListener) {
            this.responseListener = responseListener;
            return this;
        }

        public Builder pattern(Pattern pattern) {
            this.pattern = pattern;
            return this;
        }
        public Builder strategy(Strategy strategy) {
            this.strategy = strategy;
            return this;
        }
        public Builder waitTime(int seconds) {
            this.waitTimeNanos = TimeUnit.NANOSECONDS.convert(seconds, TimeUnit.SECONDS);
            return this;
        }

        public NexalithicTask build(NexalithicSession<?> targetSession) {
            if (requestAction == null) {
                throw new IllegalArgumentException("requestAction is required");
            }
            if (pattern == null) {
                throw new IllegalArgumentException("pattern is required");
            }
            if (strategy == null) {
                throw new IllegalArgumentException("strategy is required");
            }
            return new NexalithicTask(requestAction, responseAction, completeAction, timeoutAction, failedAction, cancelAction, finishAction,
                    requestListener, responseListener, pattern, strategy, waitTimeNanos, targetSession);
        }
    }
}
