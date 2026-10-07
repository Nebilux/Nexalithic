package com.nebilux.nexalithic.core.io.loop;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.infra.concurrent.atomic.AtomicStateQueue;
import com.nebilux.nexalithic.core.io.channel.LoopChannel;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * 通道循环
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class ChannelLoop<H extends ChannelLoop.Handoff, C extends NexalithicChannel> extends SelectorLoop {
    public static abstract class Options extends SelectorLoop.Options {
        public final NexalithicOption<Long> MaxIdleTimeMillis = defineOption(
                MaxIdleTimeMillis_Value(), OptionValidator.positive()
        );
        public final NexalithicOption<Integer> EventQueue_ChunkSize = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> EventQueue_DrainLimit = defineOptionLazy(content ->
                Math.max(content.getOption(EventQueue_ChunkSize) / 4, 1), OptionValidator.positive()
        );
        public final NexalithicOption<Integer> InterestQueue_ChunkSize = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> InterestQueue_DrainLimit = defineOptionLazy(context ->
                Math.max(context.getOption(InterestQueue_ChunkSize) / 4, 1), OptionValidator.positive()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
        protected abstract long MaxIdleTimeMillis_Value();
    }
    /**
     * 通道交接对象。
     *
     * <p>封装提交给 {@link ChannelLoop} 的连接资源及其生命周期信息。
     */
    public interface Handoff {
        void discard();
    }
    /**
     * ChannelLoop 内部使用的通道所有权命令。
     */
    public sealed interface Event<H extends Handoff, C extends NexalithicChannel> permits Event.Acquire, Event.Transfer, Event.Disconnect {
        void execute(ChannelLoop<H, C> owner);
        /**
         * 请求当前 Loop 接管通道资源。
         */
        record Acquire<H extends Handoff, C extends NexalithicChannel>(H handoff) implements Event<H, C> {
            @Override
            public void execute(ChannelLoop<H, C> owner) {
                owner.executeAcquireEvent(handoff);
            }
        }
        /**
         * 请求将通道所有权转移给目标 Loop。
         */
        record Transfer<H extends Handoff, C extends NexalithicChannel>(H handoff, C channel, ChannelLoop<? super H, ?> target) implements Event<H, C> {
            @Override
            public void execute(ChannelLoop<H, C> owner) {
                owner.executeTransferEvent(handoff, channel, target);
            }
        }
        /**
         * 请求断开并释放通道。
         */
        record Disconnect<H extends Handoff, C extends NexalithicChannel>(C channel, ChannelLoop.Event.Disconnect.Reason reason) implements Event<H, C> {
            public enum Reason {
                /** 用户或上层组件主动请求关闭。 */
                LOCAL_REQUEST,
                /** 对端通过协议明确请求关闭。 */
                REMOTE_REQUEST,
                /** 读取到 EOF，或者确认底层连接已经被对端关闭。 */
                REMOTE_CLOSED,
                /** Socket、Selector、读写或注册操作发生 I/O 异常。 */
                IO_FAILURE,
                /** 握手未能在规定时间内完成。 */
                HANDSHAKE_TIMEOUT,
                /** 已建立的通道长时间没有活动。 */
                IDLE_TIMEOUT,
                /** 收到非法帧、非法状态转换或不符合协议的数据。 */
                PROTOCOL_VIOLATION,
                /** 身份验证、令牌校验、密钥协商或数据认证失败。 */
                SECURITY_FAILURE,
                /** 通道向另一个 Loop 转移所有权时失败。 */
                TRANSFER_FAILURE,
                /** 当前物理连接被新的连接替换，旧连接正常退役。 */
                CHANNEL_REPLACED,
                /** Loop 正在关闭，释放其拥有的全部通道。 */
                LOOP_TERMINATING,
                /** 框架内部发生未预期错误，无法继续维护该通道。 */
                INTERNAL_FAILURE
            }

            @Override
            public void execute(ChannelLoop<H, C> owner) {
                owner.executeDisconnectEvent(channel, reason);
            }
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(ChannelLoop.class);
    private enum SubmissionState {
        OPEN,
        DRAINING,
        CLOSED
    }
    private record Constant(int EventQueue_DrainLimit, int InterestQueue_DrainLimit) {}
    private final Constant CONSTANT;
    private final AtomicStateQueue.StateHandle<SubmissionState> submissionState = new AtomicStateQueue.StateHandle<>(SubmissionState.OPEN);
    private final AtomicStateQueue<Event<H, C>, SubmissionState> eventQueue;
    private final AtomicStateQueue<LoopChannel<?, ?>, SubmissionState> interestQueue;
    private final Set<C> ownedChannels = Collections.newSetFromMap(new IdentityHashMap<>());
    public ChannelLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
        CONSTANT = context.getConstant(this.getClass(), Constant.class, () -> new Constant(
                context.getOption(options.EventQueue_DrainLimit),
                context.getOption(options.InterestQueue_DrainLimit)
        ));
        eventQueue = new AtomicStateQueue<>(
                new MpscUnboundedArrayQueue<>(context.getOption(options.EventQueue_ChunkSize)),
                submissionState, this::acceptEventSubmission
        );
        interestQueue = new AtomicStateQueue<>(
                new MpscUnboundedArrayQueue<>(context.getOption(options.InterestQueue_ChunkSize)),
                submissionState, this::acceptInterestSubmission
        );
        isDrainCondition(() -> eventQueue.isEmpty() && interestQueue.isEmpty() && ownedChannels.isEmpty());
        drainAsyncEventsCondition(() -> {
            eventQueue.drain(event -> event.execute(this), CONSTANT.EventQueue_DrainLimit());
            interestQueue.drain(LoopChannel::applyInterest, CONSTANT.InterestQueue_DrainLimit());
            return eventQueue.isEmpty() && interestQueue.isEmpty();
        });
    }

    public final boolean submitAcquireEvent(H handoff) {
        if (handoff == null) {
            throw new NullPointerException("handoff");
        }
        if (inEventLoop()) {
            if (submissionState.get() != SubmissionState.OPEN || getState().isTerminationStarted()) {
                return false;
            }
            executeAcquireEvent(handoff);
        } else {
            if (!eventQueue.offer(new Event.Acquire<>(handoff))) {
                return false;
            }
            wakeup();
        }
        return true;
    }
    public final boolean submitTransferEvent(H handoff, C channel, ChannelLoop<? super H, ?> target) {
        if (handoff == null || channel == null || target == null) {
            throw new NullPointerException("handoff, channel and target must be non-null");
        }
        if (inEventLoop()) {
            if (getState().isTerminationStarted()) {
                return false;
            }
            executeTransferEvent(handoff, channel, target);
        } else {
            if (!eventQueue.offer(new Event.Transfer<>(handoff, channel, target))) {
                return false;
            }
            wakeup();
        }
        return true;
    }
    public final boolean submitDisconnectEvent(C channel, Event.Disconnect.Reason reason) {
        if (channel == null || reason == null) {
            throw new NullPointerException("channel and reason must be non-null");
        }
        if (inEventLoop()) {
            executeDisconnectEvent(channel, reason);
            return true;
        } else {
            if (!eventQueue.offer(new Event.Disconnect<>(channel, reason))) {
                return false;
            }
            wakeup();
        }
        return true;
    }

    public final boolean submitInterestUpdate(LoopChannel<?, ?> channel) {
        if (channel == null) {
            throw new NullPointerException("channel");
        }
        if (!interestQueue.offer(channel)) {
            return false;
        }
        wakeup();
        return true;
    }

    protected final void registerOwnedChannel(C channel) {
        if (!inEventLoop()) {
            throw new IllegalStateException("Channel ownership must be registered by " + name);
        }
        if (channel.getState() != NexalithicChannel.State.Opened) {
            throw new IllegalStateException("Cannot own channel in state " + channel.getState());
        }
        if (!ownedChannels.add(channel)) {
            throw new IllegalStateException("Channel is already owned by " + name);
        }
        loadScore.increment();
    }

    protected abstract C onExecuteAcquireEvent(H handoff) throws Exception;
    /**
     * 通道已经打开并登记到当前 Loop 后的通知。
     *
     * <p>该回调执行时，通道已经属于当前 Loop，{@link #getLoadScore()} 也已经包含该通道。
     * 子类应在这里发布依赖所有权已经建立的状态，例如会话可见性或连接成功事件。</p>
     *
     * @param handoff 本次接管使用的交接对象
     * @param channel 已经登记所有权的通道
     * @throws Exception 完成接管后的发布操作失败
     */
    protected void onChannelAcquired(H handoff, C channel) throws Exception {}
    /**
     * 通道接管失败后的通知。
     *
     * <p>调用本方法前，当前 Loop 会尽力关闭已经打开的通道；如果所有权曾经登记成功，
     * 还会先按 {@link Event.Disconnect.Reason#INTERNAL_FAILURE} 执行正常断开流程。</p>
     *
     * @param handoff 本次接管使用的交接对象
     * @param failure 接管失败原因
     */
    protected void onChannelAcquireFailed(H handoff, Throwable failure) {}
    protected abstract boolean onExecuteReleaseEvent(C channel);
    protected abstract void onExecuteDisconnectEvent(C channel, Event.Disconnect.Reason reason) throws Exception;
    protected abstract void onChannelReady(SelectionKey selectionKey, C channel) throws IOException;

    @Override
    protected void sealLoop() {
        submissionState.set(getState() == State.DRAINING ? SubmissionState.DRAINING : SubmissionState.CLOSED);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final void onSelectorKeyReady(SelectionKey key) {
        Object attachment = key.attachment();
        if (!(attachment instanceof NexalithicChannel)) {
            key.cancel();
            return;
        }
        C channel = (C) attachment;
        try {
            onChannelReady(key, channel);
        } catch (Exception exception) {
            logger.error("Error while reading channel", exception);
            submitDisconnectEvent(channel, Event.Disconnect.Reason.INTERNAL_FAILURE);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void clearSelectionKey(SelectionKey key) {
        if (key.attachment() instanceof NexalithicChannel channel) {
            executeDisconnectEvent((C) channel, Event.Disconnect.Reason.LOOP_TERMINATING);
        }
    }

    @Override
    protected void discardAsyncEvents() {
        submissionState.set(SubmissionState.CLOSED);
        Event<H, C> event;
        while ((event = eventQueue.poll()) != null) {
            switch (event) {
                case Event.Acquire<?, ?> acquire -> safeDiscard(acquire.handoff());
                case Event.Transfer<?, ?> transfer -> safeDiscard(transfer.handoff());
                case Event.Disconnect<?, ?> disconnect -> {
                    @SuppressWarnings("unchecked")
                    C channel = (C) disconnect.channel();
                    executeDisconnectEvent(channel, disconnect.reason());
                }
            }
        }
        for (C channel : List.copyOf(ownedChannels)) {
            executeDisconnectEvent(channel, Event.Disconnect.Reason.LOOP_TERMINATING);
        }
    }

    private boolean acceptEventSubmission(SubmissionState submission, Event<H, C> event) {
        State lifecycle = getState();
        if (submission == SubmissionState.CLOSED || lifecycle.isTerminationStarted()) {
            return false;
        }
        if (event instanceof Event.Disconnect<?, ?>) {
            return true;
        }
        return submission == SubmissionState.OPEN && lifecycle.isAcceptingNewWork();
    }

    private boolean acceptInterestSubmission(SubmissionState submission, LoopChannel<?, ?> channel) {
        return submission != SubmissionState.CLOSED && !getState().isTerminationStarted();
    }

    private void executeAcquireEvent(H handoff) {
        C channel = null;
        boolean ownershipRegistered = false;
        try {
            channel = onExecuteAcquireEvent(handoff);
            if (channel == null) {
                return;
            }
            registerOwnedChannel(channel);
            ownershipRegistered = true;
            onChannelAcquired(handoff, channel);
        } catch (Throwable throwable) {
            Throwable failure = throwable;
            if (ownershipRegistered && channel != null) {
                executeDisconnectEvent(channel, Event.Disconnect.Reason.INTERNAL_FAILURE);
            } else if (channel != null && !ownedChannels.contains(channel)) {
                try {
                    channel.close();
                } catch (Throwable closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            try {
                onChannelAcquireFailed(handoff, failure);
            } catch (Throwable callbackFailure) {
                failure = mergeThrowable(failure, callbackFailure);
            }
            logger.error("[{}] failed to acquire channel", name, failure);
        } finally {
            if (!ownershipRegistered || channel != handoff) {
                safeDiscard(handoff);
            }
        }
    }
    private void executeTransferEvent(H handoff, C channel, ChannelLoop<? super H, ?> target) {
        if (!ownedChannels.contains(channel)) {
            logger.warn("[{}] rejected transfer of an unowned channel", name);
            safeDiscard(handoff);
            return;
        }
        boolean released = false;
        try {
            released = onExecuteReleaseEvent(channel);
            if (!released) {
                executeDisconnectEvent(channel, Event.Disconnect.Reason.TRANSFER_FAILURE);
                if (channel != handoff) {
                    safeDiscard(handoff);
                }
                return;
            }
            if (!ownedChannels.remove(channel)) {
                throw new IllegalStateException("Channel ownership disappeared during transfer");
            }
            loadScore.decrement();
            if (!target.submitAcquireEvent(handoff)) {
                safeDiscard(handoff);
            }
        } catch (Throwable failure) {
            logger.error("[{}] failed to transfer channel", name, failure);
            if (!released && ownedChannels.contains(channel)) {
                executeDisconnectEvent(channel, Event.Disconnect.Reason.TRANSFER_FAILURE);
            } else {
                safeDiscard(handoff);
            }
        }
    }
    private void executeDisconnectEvent(C channel, Event.Disconnect.Reason reason) {
        boolean owned = ownedChannels.remove(channel);
        Throwable failure = null;
        try {
            channel.close();
        } catch (Throwable throwable) {
            failure = throwable;
        }
        if (owned) {
            try {
                onExecuteDisconnectEvent(channel, reason);
            } catch (Throwable throwable) {
                failure = mergeThrowable(failure, throwable);
            } finally {
                loadScore.decrement();
            }
        }
        if (failure != null) {
            logger.error("[{}] failed to disconnect channel", name, failure);
        }
    }

    private void safeDiscard(Handoff handoff) {
        try {
            handoff.discard();
        } catch (Throwable failure) {
            logger.error("[{}] failed to discard handoff", name, failure);
        }
    }
}
