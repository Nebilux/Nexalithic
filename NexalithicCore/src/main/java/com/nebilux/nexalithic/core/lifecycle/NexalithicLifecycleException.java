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

package com.nebilux.nexalithic.core.lifecycle;

import com.nebilux.nexalithic.core.exception.NexalithicFailure;
import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 生命周期操作失败的非受检异常。
 *
 * <p>例如终端启动或关闭阶段发生异常，调用方等待该阶段结果时会收到此异常；
 * 底层失败原因可通过 {@link #getCause()} 获取。</p>
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class NexalithicLifecycleException extends NexalithicUncheckedException {
    public NexalithicLifecycleException(String message) {
        super(message);
    }

    public NexalithicLifecycleException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NexalithicLifecycleException forEndpointOperation(Class<?> endpointType, String action, Throwable cause) {
        return new NexalithicLifecycleException(NexalithicFailure.targetFailedTo(endpointType.getSimpleName(), action), cause);
    }
}
