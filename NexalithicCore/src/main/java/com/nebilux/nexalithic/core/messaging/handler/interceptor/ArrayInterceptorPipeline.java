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
 * 基于数组快照的 Handler 拦截器管线。
 *
 * <p>构造时会把拦截器列表复制为数组，避免执行过程中受外部列表修改影响。
 * 前置回调按数组正序执行，后置和完成回调按数组逆序执行。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
 */
class ArrayInterceptorPipeline<HC extends HandlerContext<?>> implements InterceptorPipeline <HC>{
    private final HandlerMetadata metadata;
    private final HandlerInterceptor<HC>[] interceptors;

    /**
     * 创建数组拦截器管线。
     *
     * @param metadata Handler 元数据
     * @param interceptors 拦截器列表
     */
    @SuppressWarnings("unchecked")
    public ArrayInterceptorPipeline(HandlerMetadata metadata, List<HandlerInterceptor<HC>> interceptors) {
        this.metadata = metadata;
        this.interceptors = (HandlerInterceptor<HC>[]) interceptors.toArray(HandlerInterceptor[]::new);
    }

    @Override
    public boolean applyBefore(HC context) throws Exception {
        int i = 0;
        try {
            while (i < interceptors.length) {
                if (!interceptors[i].onBefore(context, metadata)) {
                    complete(context, null, i);
                    return false;
                }
                i++;
            }
            return true;
        } catch (Exception e) {
            complete(context, e, i);
            throw e;
        }
    }

    @Override
    public void applyAfter(HC context) throws Exception {
        for (int i = interceptors.length - 1; i >= 0; i--) {
            interceptors[i].onAfter(context, metadata);
        }
    }

    @Override
    public void complete(HC context, Exception exception) {
        complete(context, exception, interceptors.length - 1);
    }

    private void complete(HC context, Exception exception, int lastPassedIndex) {
        for (; lastPassedIndex >= 0; lastPassedIndex--) {
            interceptors[lastPassedIndex].onCompletion(context, metadata, exception);
        }
    }
}
