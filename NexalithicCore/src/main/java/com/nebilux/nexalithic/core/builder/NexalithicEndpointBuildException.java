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

package com.nebilux.nexalithic.core.builder;

import com.nebilux.nexalithic.core.exception.NexalithicCheckedException;
import com.nebilux.nexalithic.core.exception.NexalithicFailure;

/**
 * 构建客户端或服务器终端失败的受检异常。
 *
 * <p>通常发生在 {@code build()} 组装组件、分配资源或执行配置回调时；
 * 原始异常可通过 {@link #getCause()} 获取。</p>
 */
public final class NexalithicEndpointBuildException extends NexalithicCheckedException {
    private NexalithicEndpointBuildException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NexalithicEndpointBuildException forTarget(Class<?> target, Throwable cause) {
        return new NexalithicEndpointBuildException(NexalithicFailure.failedTo("build", target.getSimpleName()), cause);
    }
}
