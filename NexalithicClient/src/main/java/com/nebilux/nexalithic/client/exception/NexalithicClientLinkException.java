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

package com.nebilux.nexalithic.client.exception;

import com.nebilux.nexalithic.core.exception.NexalithicCheckedException;
import com.nebilux.nexalithic.core.exception.NexalithicFailure;

import java.net.InetSocketAddress;

/**
 * 客户端建立连接或完成安全握手失败的受检异常。
 *
 * <p>调用 {@code link()} 时发生网络 I/O 或安全相关异常会被包装为此类型；
 * 原始异常可通过 {@link #getCause()} 获取。</p>
 *
 * @author Reonvia
 * @since 0.3.1
 */
public final class NexalithicClientLinkException extends NexalithicCheckedException {
    private NexalithicClientLinkException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NexalithicClientLinkException forRemote(InetSocketAddress remote, Throwable cause) {
        return new NexalithicClientLinkException(NexalithicFailure.failedTo("link to", remote.toString()), cause);
    }
}
