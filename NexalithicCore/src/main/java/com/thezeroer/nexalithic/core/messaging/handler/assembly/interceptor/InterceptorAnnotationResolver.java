package com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor;

import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

import java.lang.annotation.Annotation;

/**
 * 拦截器注解解析门面。
 *
 * <p>Controller 装配器发现一条 {@code InterceptorBinding} 后，会把业务注解实例和
 * 绑定的拦截器类型交给该接口。实现类负责选择合适的解析策略，并返回拦截器工厂可消费的
 * {@link InterceptorConfigurationBinding}。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/29
 */
public interface InterceptorAnnotationResolver {
    /**
     * 解析指定类型的拦截器注解。
     *
     * @param annotation 目标注解
     * @param interceptorType 注解绑定的拦截器类型
     * @return 拦截器配置绑定
     */
    InterceptorConfigurationBinding resolve(Annotation annotation, Class<? extends HandlerInterceptor<?>> interceptorType);
}
