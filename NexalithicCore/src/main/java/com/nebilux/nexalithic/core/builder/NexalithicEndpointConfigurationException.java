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

import com.nebilux.nexalithic.core.exception.NexalithicFailure;
import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 终端 Builder 配置失败的非受检异常。
 *
 * <p>在面向用户的配置方法执行配置动作或回调时，如果普通运行时异常需要转换为框架异常，
 * 会以此异常包装；原始异常可通过 {@link #getCause()} 获取。</p>
 */
public final class NexalithicEndpointConfigurationException extends NexalithicUncheckedException {
    private NexalithicEndpointConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NexalithicEndpointConfigurationException forScope(String scope, Throwable cause) {
        return new NexalithicEndpointConfigurationException(NexalithicFailure.failedTo("configure", scope), cause);
    }
}
