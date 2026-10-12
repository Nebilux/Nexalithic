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

/**
 * Nexalithic 非受检异常基类。
 *
 * <p>用于配置无效、状态不允许当前操作、注册冲突等无需调用者强制捕获的失败；
 * 具体原因由子类说明。用户回调自行抛出的异常不一定属于此体系。</p>
 *
 * @author Reonvia
 * @since 0.4.0
 */
public abstract class NexalithicUncheckedException extends RuntimeException implements NexalithicFailure {
    protected NexalithicUncheckedException(String message) {
        super(message);
    }

    protected NexalithicUncheckedException(String message, Throwable cause) {
        super(message, cause);
    }

    protected NexalithicUncheckedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    protected NexalithicUncheckedException(String message, boolean writableStackTrace) {
        super(message, null, false, writableStackTrace);
    }
}
