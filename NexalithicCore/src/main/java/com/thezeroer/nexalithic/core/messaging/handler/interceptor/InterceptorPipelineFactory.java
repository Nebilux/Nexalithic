package com.thezeroer.nexalithic.core.messaging.handler.interceptor;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerMetadata;

import java.util.List;

/**
 * 拦截器管线工厂。
 *
 * <p>根据拦截器列表创建最合适的管线实现。空列表或 {@code null}
 * 会复用 {@link EmptyInterceptorPipeline} 单例，非空列表会创建数组快照管线。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/09
 */
public class InterceptorPipelineFactory {
    private InterceptorPipelineFactory() {}

    @SuppressWarnings("unchecked")
    public static <HC extends HandlerContext<?>> InterceptorPipeline<HC> create(HandlerMetadata metadata, List<HandlerInterceptor<HC>> interceptors) {
        if (interceptors == null || interceptors.isEmpty()) {
            return (InterceptorPipeline<HC>) EmptyInterceptorPipeline.getInstance();
        }
        if (metadata == null) {
            throw new NullPointerException("HandlerMetadata is null");
        }
        return new ArrayInterceptorPipeline<>(metadata, interceptors);
    }
}
