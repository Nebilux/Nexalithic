package com.thezeroer.nexalithic.core.event;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nexalithic 事件总线
 * <p>
 * 该总线负责管理事件主题 {@link EventTopic} 的注册与订阅。
 * 为了实现极致性能，发布侧直接持有 {@code EventTopic} 句柄进行判定与发布。
 * </p>
 * <p>
 * <ul>
 * <li><b>同步触发：</b>事件发布（Publish）与执行（Handle）是在同一个线程内<b>同步</b>完成的。</li>
 * <li><b>线程阻塞：</b>若订阅者回调中包含耗时操作（如 I/O、数据库访问、复杂计算），将直接阻塞发布者线程。</li>
 * <li><b>异步建议：</b>对于耗时任务，开发者应在订阅者逻辑中自行提交至线程池（ExecutorService）执行。</li>
 * </ul>
 * </p>
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/19
 * @version 1.0.0
 */
public class NexalithicEventBus {
    private final Map<Class<? extends NexalithicEvent>, EventTopic<? extends NexalithicEvent>> topics = new ConcurrentHashMap<>();

    /**
     * 注册方法：强制要求 Class<T> 与 EventTopic<T> 的 T 必须相同
     */
    @SuppressWarnings("unchecked")
    public <T extends NexalithicEvent> EventTopic<T> registerTopic(Class<T> eventClass) {
        return (EventTopic<T>) topics.computeIfAbsent(eventClass, k -> new EventTopic<T>());
    }

    /**
     * 订阅事件。
     * <p>注意：处理器将在发布者线程中同步执行。</p>
     *
     * @param eventClass 订阅的事件类
     * @param handler    事件处理器
     * @return 用于注销的句柄
     * @throws IllegalArgumentException 如果指定的事件主题未注册
     */
    public <T extends NexalithicEvent> EventSubscription subscribe(Class<T> eventClass, EventHandler<T> handler) {
        EventTopic<T> topic = getTopic(eventClass);
        if (topic == null) {
            throw new IllegalArgumentException("No such topic for event " + eventClass.getName());
        }
        return topic.subscribe(handler);
    }

    /**
     * 取消订阅指定的处理器。
     *
     * @param eventClass 事件类
     * @param handler    原处理器引用
     */
    public <T extends NexalithicEvent> void unsubscribe(Class<T> eventClass, EventHandler<T> handler) {
        EventTopic<T> topic = getTopic(eventClass);
        if (topic == null) {
            throw new IllegalArgumentException("No such topic for event " + eventClass.getName());
        }
        topic.unsubscribe(handler);
    }

    /**
     * 获取方法：通过 Class<T> 返回对应的 EventTopic<T>
     */
    @SuppressWarnings("unchecked")
    private <T extends NexalithicEvent> EventTopic<T> getTopic(Class<T> eventClass) {
        return (EventTopic<T>) topics.get(eventClass);
    }
}
