package com.nebilux.nexalithic.core.event;

/**
 * 订阅任务句柄，用于取消订阅
 *
 * @author Reonvia
 * @since 0.1.0
 */
@FunctionalInterface
public interface EventSubscription {
    /**
     * 取消当前订阅
     */
    void unsubscribe();
}