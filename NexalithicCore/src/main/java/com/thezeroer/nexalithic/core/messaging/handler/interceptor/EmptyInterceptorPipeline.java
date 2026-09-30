package com.thezeroer.nexalithic.core.messaging.handler.interceptor;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

/**
 * 空拦截器管线。
 *
 * <p>当 Handler 未配置任何拦截器时使用该单例实现，避免为每个 Handler 创建无意义的空数组。
 * 所有回调均为空操作，前置检查始终允许 Handler 执行。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/09
 */
class EmptyInterceptorPipeline implements InterceptorPipeline<HandlerContext<?>> {
    private static final EmptyInterceptorPipeline instance = new EmptyInterceptorPipeline();

    /**
     * 创建空管线单例。
     */
    private EmptyInterceptorPipeline() {}

    @Override
    public boolean applyBefore(HandlerContext<?> context) {
        return true;
    }

    @Override
    public void applyAfter(HandlerContext<?> context) {
    }

    @Override
    public void complete(HandlerContext<?> context, Exception exception) {
    }

    /**
     * 返回空管线单例。
     *
     * @return 空管线单例
     */
    public static InterceptorPipeline<HandlerContext<?>> getInstance() {
        return instance;
    }
}
