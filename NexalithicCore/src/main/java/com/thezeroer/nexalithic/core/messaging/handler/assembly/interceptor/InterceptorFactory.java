package com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

/**
 * 拦截器实例工厂。
 *
 * <p>负责根据拦截器配置绑定取得对应的拦截器实例。
 * 返回值可以是新创建的实例，也可以是缓存或共享实例。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/29
 */
public interface InterceptorFactory<HC extends HandlerContext<?>> {

    /**
     * 根据配置绑定取得拦截器实例。
     *
     * @param binding 拦截器配置绑定
     * @return 可用于目标 Handler 的拦截器实例
     */
    HandlerInterceptor<HC> get(InterceptorConfigurationBinding binding);
}
