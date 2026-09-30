package com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.builtin;

/**
 * 拦截器实例作用域。
 *
 * <p>作用域描述拦截器实例在装配阶段的创建与复用策略。
 * 它由 {@link InterceptorCreator} 声明，并由 {@link DefaultInterceptorFactory}
 * 执行具体缓存策略。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/29
 */
public enum InterceptorScope {

    /**
     * 每个配置绑定拥有一个独立实例。
     *
     * <p>适用于拦截器实例持有注解参数、Handler 相关状态或其他不能跨绑定共享的数据。</p>
     */
    PER_BINDING,

    /**
     * 相同拦截器类型共享同一个实例。
     *
     * <p>适用于无状态、线程安全，或内部状态可安全复用的拦截器。</p>
     */
    SINGLETON
}
