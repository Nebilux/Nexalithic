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

import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;
import com.nebilux.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

/**
 * 拦截器实例工厂。
 *
 * <p>负责根据拦截器配置绑定取得对应的拦截器实例。
 * 返回值可以是新创建的实例，也可以是缓存或共享实例。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
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
