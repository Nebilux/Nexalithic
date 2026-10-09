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

package com.nebilux.nexalithic.core.exception;

import java.util.Objects;

/**
 * 标记由 Nexalithic 定义的受检与非受检异常。
 *
 * <p>此接口不是 {@link Throwable} 的子类型，不能用作 {@code catch} 或 {@code throws} 的类型。
 * 该标记适用于框架定义的操作失败；JDK 参数前置条件异常、虚拟机错误及用户回调自行抛出的异常
 * 并不保证实现此接口。包装后的原始异常可通过 {@link Throwable#getCause()} 查看。</p>
 *
 * @author Reonvia
 * @since 0.3.1
 */
public interface NexalithicFailure {
    /** 操作在前的消息形式，例如 {@code Failed to build NexalithicEndpoint}。 */
    static String failedTo(String action, String target) {
        return "Failed to " + Objects.requireNonNull(action, "action") + " "
                + Objects.requireNonNull(target, "target");
    }

    /** 对象在前的消息形式，例如 {@code NexalithicEndpoint failed to start}。 */
    static String targetFailedTo(String target, String action) {
        return Objects.requireNonNull(target, "target") + " failed to "
                + Objects.requireNonNull(action, "action");
    }
}
