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

package com.nebilux.nexalithic.core.messaging.handler.interceptor;

import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;
import com.nebilux.nexalithic.core.messaging.handler.HandlerMetadata;

import java.util.List;

/**
 * 拦截器管线工厂。
 *
 * <p>根据拦截器列表创建最合适的管线实现。空列表或 {@code null}
 * 会复用 {@link EmptyInterceptorPipeline} 单例，非空列表会创建数组快照管线。</p>
 *
 * @author Reonvia
 * @since 0.2.0
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
