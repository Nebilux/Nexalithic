package com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.builtin;

import com.thezeroer.nexalithic.core.exception.NexalithicDuplicateKeyException;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorConfigurationBinding;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorFactory;
import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 默认拦截器实例工厂。
 *
 * <p>该工厂按拦截器实现类型查找对应的 {@link InterceptorCreator}，
 * 再根据创建器声明的 {@link InterceptorScope} 决定是否复用实例。</p>
 *
 * <p>{@link InterceptorScope#SINGLETON} 的实例以拦截器类型为 key 缓存在本工厂内；
 * {@link InterceptorScope#PER_BINDING} 每次调用都会委托创建器创建新实例。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/29
 */
public class DefaultInterceptorFactory<HC extends HandlerContext<?>> implements InterceptorFactory<HC> {
    private final Map<Class<? extends HandlerInterceptor<?>>, InterceptorCreator<HC>> creators;
    private final Map<Class<? extends HandlerInterceptor<?>>, HandlerInterceptor<HC>> caches;

    /**
     * 创建默认拦截器工厂。
     *
     * @param creators 拦截器类型到创建器的映射
     */
    public DefaultInterceptorFactory(Map<Class<? extends HandlerInterceptor<?>>, InterceptorCreator<HC>> creators) {
        this.creators = Map.copyOf(creators);
        this.caches = new LinkedHashMap<>();
    }

    /**
     * 创建默认拦截器工厂构建器。
     *
     * @param <HC> Handler 上下文类型
     * @return 构建器
     */
    public static <HC extends HandlerContext<?>> Builder<HC> builder() {
        return new Builder<>();
    }

    /**
     * 根据配置绑定取得拦截器实例。
     *
     * @param binding 拦截器配置绑定
     * @return 可用于目标 Handler 的拦截器实例
     * @throws IllegalStateException 未注册目标拦截器类型的创建器时抛出
     */
    @Override
    public HandlerInterceptor<HC> get(InterceptorConfigurationBinding binding) {
        InterceptorCreator<HC> creator = creators.get(binding.interceptorType());
        if (creator == null) {
            throw new IllegalStateException("No InterceptorCreator for " + binding.interceptorType().getName());
        }
        if (creator.scope() == InterceptorScope.SINGLETON) {
            return caches.computeIfAbsent(binding.interceptorType(), ignored -> creator.create(binding));
        }
        return creator.create(binding);
    }

    /**
     * {@link DefaultInterceptorFactory} 构建器。
     *
     * @param <HC> Handler 上下文类型
     */
    public static class Builder<HC extends HandlerContext<?>> {
        private final Map<Class<? extends HandlerInterceptor<?>>, InterceptorCreator<HC>> creators = new LinkedHashMap<>();
        private Builder() {}

        /**
         * 注册一个拦截器创建器。
         *
         * @param creator 拦截器创建器
         * @return 当前构建器
         * @throws NexalithicDuplicateKeyException 重复注册同一拦截器类型时抛出
         */
        public Builder<HC> creator(InterceptorCreator<HC> creator) {
            if (creators.putIfAbsent(creator.type(),  creator) != null) {
                throw new NexalithicDuplicateKeyException("creators", creator);
            }
            return this;
        }

        /**
         * 批量注册拦截器创建器。
         *
         * @param creators 拦截器创建器集合
         * @return 当前构建器
         */
        public Builder<HC> creator(Collection<InterceptorCreator<HC>> creators) {
            for (InterceptorCreator<HC> creator : creators) {
                creator(creator);
            }
            return this;
        }

        /**
         * 构建默认拦截器工厂。
         *
         * @return 默认拦截器工厂
         */
        public DefaultInterceptorFactory<HC> build() {
            return new DefaultInterceptorFactory<>(creators);
        }
    }
}
