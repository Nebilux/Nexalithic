package com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor;

import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

/**
 * 拦截器配置绑定。
 *
 * <p>该记录把注解解析得到的 {@link InterceptorConfiguration}
 * 与需要创建的拦截器实现类型关联起来，是装配器、注解解析器和拦截器工厂之间传递的核心数据。</p>
 *
 * @param configuration 拦截器配置
 * @param interceptorType 目标拦截器实现类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/19
 */
public record InterceptorConfigurationBinding(
        InterceptorConfiguration configuration,
        Class<? extends HandlerInterceptor<?>> interceptorType) {
}
