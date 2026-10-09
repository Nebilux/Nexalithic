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

package com.nebilux.nexalithic.core.session;

import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 当前会话或会话管理器状态不允许所请求操作的异常。
 *
 * <p>例如管理器尚未运行、在非空闲状态下尝试建立连接、已有活动会话，
 * 建连期间状态被并发更改，或调用需要活动会话的客户端方法时没有会话。</p>
 */
public final class NexalithicSessionException extends NexalithicUncheckedException {
    private NexalithicSessionException(String message) {
        super(message);
    }

    public static NexalithicSessionException managerNotRunning() {
        return new NexalithicSessionException("SessionManager is not running");
    }

    public static NexalithicSessionException invalidConnectionState(Enum<?> current, Enum<?> required) {
        return new NexalithicSessionException(
                "Cannot establish a session while in State " + current + ", must be " + required);
    }

    public static NexalithicSessionException alreadyActive() {
        return new NexalithicSessionException("A client session is already active");
    }

    public static NexalithicSessionException stateChangedDuringConnection() {
        return new NexalithicSessionException("Client session state changed while establishing a connection");
    }

    public static NexalithicSessionException noActiveClientSession() {
        return new NexalithicSessionException("No active client session");
    }
}
