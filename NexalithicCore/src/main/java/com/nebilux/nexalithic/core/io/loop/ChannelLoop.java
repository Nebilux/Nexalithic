package com.nebilux.nexalithic.core.io.loop;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.io.channel.LoopChannel;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 通道循环
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/08
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
    private record Constant(int EventQueue_DrainLimit, int InterestQueue_DrainLimit) {}
    private final Constant CONSTANT;
    private final MpscUnboundedArrayQueue<Event<H, C>> eventQueue;
    private final MpscUnboundedArrayQueue<LoopChannel<?, ?>> interestQueue;
    public ChannelLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
        CONSTANT = context.getConstant(this.getClass(), Constant.class, () -> new Constant(
                context.getOption(options.EventQueue_DrainLimit),
                context.getOption(options.InterestQueue_DrainLimit)
        ));
        eventQueue = new MpscUnboundedArrayQueue<>(context.getOption(options.EventQueue_ChunkSize));
        interestQueue = new MpscUnboundedArrayQueue<>(context.getOption(options.InterestQueue_ChunkSize));
        isDrainCondition(() -> eventQueue.isEmpty() && interestQueue.isEmpty());
        drainAsyncEventsCondition(() -> {
            eventQueue.drain(event -> event.execute(this), CONSTANT.EventQueue_DrainLimit());
            interestQueue.drain(LoopChannel::applyInterest, CONSTANT.InterestQueue_DrainLimit());
            return eventQueue.isEmpty() && interestQueue.isEmpty();
        });
    }

    public final boolean submitAcquireEvent(H handoff) {
        if (isSealed()) {
            return false;
        }
        if (inEventLoop()) {
            executeAcquireEvent(handoff);
        } else {
            eventQueue.add(new Event.Acquire<>(handoff));
            wakeup();
        }
        return true;
    }
    public final boolean submitTransferEvent(H handoff, C channel, ChannelLoop<? super H, ?> target) {
        if (isSealed()) {
            return false;
        }
        if (inEventLoop()) {
            executeTransferEvent(handoff, channel, target);
        } else {
            eventQueue.add(new Event.Transfer<>(handoff, channel, target));
            wakeup();
        }
        return true;
    }
    public final void submitDisconnectEvent(C channel, Event.Disconnect.Reason reason) {
        if (inEventLoop()) {
            executeDisconnectEvent(channel, reason);
        } else {
            eventQueue.add(new Event.Disconnect<>(channel, reason));
            wakeup();
        }
    }

    public final void submitInterestUpdate(LoopChannel<?, ?> channel) {
        interestQueue.add(channel);
        wakeup();
    }

    protected abstract boolean onExecuteAcquireEvent(H handoff);
    protected abstract boolean onExecuteReleaseEvent(C channel);
    protected abstract boolean onExecuteDisconnectEvent(C channel, Event.Disconnect.Reason reason);
    protected abstract void onChannelReady(SelectionKey selectionKey, C channel) throws IOException;

    @Override
    @SuppressWarnings("unchecked")
    protected final void onSelectorKeyReady(SelectionKey key) {
        C channel = (C) key.attachment();
        channel.updateLastActiveTimeNanos(System.nanoTime());
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
        eventQueue.drain(event -> {
            switch (event) {
                case Event.Acquire<?, ?> acquire -> acquire.handoff().discard();
                case Event.Transfer<?, ?> transfer -> transfer.handoff().discard();
                default -> {}
            }
        }, Integer.MAX_VALUE);
        interestQueue.clear();
    }

    private void executeAcquireEvent(H handoff) {
        try {
            if (onExecuteAcquireEvent(handoff)) {
                loadScore.increment();
            }
        } catch (Exception failure) {
            logger.error("[{}] failed to acquire channel", name, failure);
            handoff.discard();
        }
    }
    private void executeTransferEvent(H handoff, C channel, ChannelLoop<? super H, ?> target) {
        try {
            if (onExecuteReleaseEvent(channel)) {
                loadScore.decrement();
                target.submitAcquireEvent(handoff);
            } else {
                executeDisconnectEvent(channel, Event.Disconnect.Reason.TRANSFER_FAILURE);
            }
        } catch (Exception failure) {
            logger.error("[{}] failed to transfer channel", name, failure);
        }
    }
    private void executeDisconnectEvent(C channel, Event.Disconnect.Reason reason) {
        try {
            if (channel.close() && onExecuteDisconnectEvent(channel, reason)) {
                loadScore.decrement();
            }
        } catch (Exception failure) {
            logger.error("[{}] failed to disconnect channel", name, failure);
        }
    }
}
