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

package com.nebilux.nexalithic.core.messaging.handler.assembly.interceptor;

import com.nebilux.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

/**
 * 拦截器配置绑定。
 *
 * <p>该记录把注解解析得到的 {@link InterceptorConfiguration}
 * 与需要创建的拦截器实现类型关联起来，是装配器、注解解析器和拦截器工厂之间传递的核心数据。</p>
 *
 * @param configuration 拦截器配置
 * @param interceptorType 目标拦截器实现类型
 *
 * @author Reonvia
 * @since 0.2.0
 */
public record InterceptorConfigurationBinding(
        InterceptorConfiguration configuration,
        Class<? extends HandlerInterceptor<?>> interceptorType) {
}
