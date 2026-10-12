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

package com.nebilux.nexalithic.server.exception;

import com.nebilux.nexalithic.core.exception.NexalithicCheckedException;
import com.nebilux.nexalithic.core.exception.NexalithicFailure;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;

import java.net.InetSocketAddress;

/**
 * 服务器打开或注册监听器失败的受检异常。
 *
 * <p>通常发生在创建监听套接字、绑定本地地址，或将监听器交给接入循环注册时；
 * 底层 I/O 异常可通过 {@link #getCause()} 获取。</p>
 *
 * @author Reonvia
 * @since 0.4.0
 */
public final class NexalithicServerOpenException extends NexalithicCheckedException {
    private NexalithicServerOpenException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NexalithicServerOpenException forListener(InetSocketAddress address, NexalithicChannel.Kind kind, Throwable cause) {
        return new NexalithicServerOpenException(NexalithicFailure.failedTo("open", "listener at " + address + " for " + kind), cause);
    }
}
