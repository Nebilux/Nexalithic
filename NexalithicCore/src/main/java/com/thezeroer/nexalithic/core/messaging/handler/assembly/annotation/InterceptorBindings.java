package com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link InterceptorBinding} 的容器注解。
 *
 * <p>用于在同一个业务注解类型上声明多条拦截器绑定，使一个业务注解可以同时触发
 * 多个 {@code HandlerInterceptor}。装配器通过
 * {@link Class#getDeclaredAnnotationsByType(Class)} 读取这些绑定。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/28
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface InterceptorBindings {

    /**
     * 当前注解类型声明的所有拦截器绑定。
     *
     * @return 拦截器绑定数组
     */
    InterceptorBinding[] value();
}
