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

package com.nebilux.nexalithic.core.messaging.task;

import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 任务配置或登记不符合要求的异常。
 *
 * <p>例如构建任务时缺少必要的动作、模式或策略，提交的任务属于其他会话，
 * 或同一任务标识已经登记。</p>
 */
public final class NexalithicTaskException extends NexalithicUncheckedException {
    private NexalithicTaskException(String message) {
        super(message);
    }

    public static NexalithicTaskException requiredField(String field) {
        return new NexalithicTaskException(field + " is required");
    }

    public static NexalithicTaskException targetSessionMismatch() {
        return new NexalithicTaskException("Target Session Mismatch");
    }

    public static NexalithicTaskException alreadyRegistered(long taskId) {
        return new NexalithicTaskException("Task " + taskId + " already registered");
    }
}
