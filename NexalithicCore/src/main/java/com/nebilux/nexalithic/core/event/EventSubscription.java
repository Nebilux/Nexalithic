package com.nebilux.nexalithic.core.event;

/**
 * 订阅任务句柄，用于取消订阅
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/19
 * @version 1.0.0
 */
@FunctionalInterface
public interface EventSubscription {
    /**
     * 取消当前订阅
     */
    void unsubscribe();
}