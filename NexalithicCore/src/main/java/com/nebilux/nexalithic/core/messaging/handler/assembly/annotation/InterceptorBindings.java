/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.messaging.handler.assembly.annotation;

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
 * @author Reonvia
 * @since 0.2.0
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
