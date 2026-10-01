package com.nebilux.nexalithic.client.manager;

import com.nebilux.nexalithic.client.NexalithicClient;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.event.EventDefinition;
import com.nebilux.nexalithic.core.event.EventTopic;
import com.nebilux.nexalithic.core.event.NexalithicEvent;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 链接状态管理器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class LinkStatusManager {
    /**
     * 链路状态。
     * <p>
     * 该枚举描述了客户端与服务器之间链路的逻辑生存周期，屏蔽了底层多通道（信令/业务）的构建细节。
     * </p>
     *
     * @author Reonvia
     * @since 0.1.0
     */
    public enum Status {
        /**
         * 未连接。
         * <p>初始状态或连接已彻底断开（包括主动关闭或重连失败）。</p>
         */
        UNLINKED,

        /**
         * 连接中。
         * <p>正在执行建立物理连接、安全协议握手以及内部多通道协商等过程。</p>
         */
        LINKING,

        /**
         * 已连接。
         * <p>全链路已就绪，业务通道已对齐，开发者可以正常进行业务交互。</p>
         */
        LINKED,

        /**
         * 正在重连。
         * <p>链路发生非预期中断，框架正在尝试自动恢复连接。
         * 此时请求可能会被挂起或根据配置直接失败。</p>
         */
        RECONNECTING
    }
    /**
     * 原因
     *
     * @author Reonvia
     * @since 0.1.0
     */
    public enum Reason {
        /** 无原因或未知原因 */
        NONE,
        /** 客户端主动关闭 */
        LOCAL_ACTIVE,
        /** 服务器主动断开 */
        REMOTE_ACTIVE,
        /** 网络异常（如超时、物理断网、心跳丢失） */
        NETWORK_ERROR,
        /** 协议错误（如密钥协商失败、非法数据包） */
        PROTOCOL_ERROR,
    }
    public static class Events extends EventDefinition {
        public record StatusTransition(Status from, Status to, Reason reason, Object attachment) implements NexalithicEvent {}
        private final EventTopic<StatusTransition> statusTransitionTopic;
        public Events(EventTopic<StatusTransition> statusTransitionTopic) {
            this.statusTransitionTopic = statusTransitionTopic;
        }
    }
    private final AtomicReference<Status> linkStatus = new AtomicReference<>(Status.UNLINKED);
    private final Events events;

    public LinkStatusManager(NexalithicBuilderContext context) {
        NexalithicEventBus eventBus = context.getModule(NexalithicClient.MODULES.EventBus);
        events = new Events(
                eventBus.registerTopic(Events.StatusTransition.class)
        );
    }

    /**
     * 触发状态转移事件。
     *
     * @param to   实际发生的结束状态
     * @param reason 原因
     */
    public void trigger(Status to, Reason reason, Object attachment) {
        Status from = linkStatus.get();
        if (from == to) {
            return;
        }
        if (!linkStatus.compareAndSet(from, to)) {
            trigger(to, reason, attachment);
            return;
        }
        if (events.statusTransitionTopic.isSubscribed()) {
            events.statusTransitionTopic.publish(new Events.StatusTransition(from, to, reason, attachment));
        }
    }
    public void trigger(Status to, Reason reason) {
        trigger(to, reason, null);
    }
    public void trigger(Status to, Object attachment) {
        trigger(to, Reason.NONE, attachment);
    }
    public void trigger(Status to) {
        trigger(to, Reason.NONE, null);
    }

    public Status getStatus() {
        return linkStatus.get();
    }
}
