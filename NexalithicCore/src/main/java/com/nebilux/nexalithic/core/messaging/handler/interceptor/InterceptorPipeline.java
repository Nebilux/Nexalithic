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

/**
 * Handler 拦截器执行管线。
 *
 * <p>管线在 Handler 装配阶段创建，装配后不可变，
 * 可供同一个 Handler 的多个并发请求复用。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
 */
public interface InterceptorPipeline<HC extends HandlerContext<?>> {
    /**
     * 正序执行所有拦截器的 onBefore。
     *
     * <p>如果某个拦截器返回 false，本方法会立即对之前已经
     * 返回 true 的拦截器逆序执行 onCompletion，然后返回 false。</p>
     *
     * <p>如果某个 onBefore 抛出异常，也会先对之前已经通过的
     * 拦截器执行 onCompletion，再将原异常抛出。</p>
     *
     * @param context 当前请求上下文
     * @return true 表示允许继续执行 Handler；false 表示请求已被拦截
     * @throws Exception 前置拦截器抛出的异常
     */
    boolean applyBefore(HC context) throws Exception;

    /**
     * Handler 正常完成后，逆序执行 onAfter。
     *
     * @param context 当前请求上下文
     * @throws Exception 后置拦截器抛出的异常
     */
    void applyAfter(HC context) throws Exception;

    /**
     * Handler 整体调用结束后，逆序执行 onCompletion。
     *
     * <p>只有在 applyBefore 返回 true 后，外部才应调用本方法。</p>
     *
     * @param context 当前请求上下文
     * @param exception Handler 调用过程中捕获的异常；正常完成时为 {@code null}
     */
    void complete(HC context, Exception exception);
}
