package com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorConfigurationBinding;
import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

/**
 * 拦截器实例创建器。
 *
 * <p>创建器封装某一种 {@link HandlerInterceptor} 的实例化逻辑。
 * 它负责解释 {@link InterceptorConfigurationBinding} 中的配置并创建类型兼容的拦截器，
 * 但实例是否缓存由 {@link DefaultInterceptorFactory} 根据 {@link #scope()} 统一决定。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/11
 */
public interface InterceptorCreator<HC extends HandlerContext<?>> {

    /**
     * 根据配置绑定创建拦截器。
     *
     * @param configurationBinding 配置绑定
     * @return 创建的拦截器
     */
    HandlerInterceptor<HC> create(InterceptorConfigurationBinding configurationBinding);

    /**
     * 返回当前创建器负责创建的拦截器类型。
     *
     * @return 拦截器实现类型
     */
    Class<? extends HandlerInterceptor<HC>> type();

    /**
     * 返回当前拦截器类型的实例作用域。
     *
     * <p>默认每个配置绑定创建独立实例。无状态或线程安全的拦截器可以覆盖为
     * {@link InterceptorScope#SINGLETON}。</p>
     *
     * @return 拦截器实例作用域
     */
    default InterceptorScope scope() {
        return InterceptorScope.PER_BINDING;
    }
}
