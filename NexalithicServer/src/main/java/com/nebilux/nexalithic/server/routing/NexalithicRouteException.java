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

package com.nebilux.nexalithic.server.routing;

import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 服务器路由目标或 CIDR 条目无效的异常。
 *
 * <p>例如路由使用通配地址、端口不在有效范围内，或 CIDR 的格式、地址、
 * 前缀不合法。地址或前缀解析失败时，底层异常保留为 cause。</p>
 */
public final class NexalithicRouteException extends NexalithicUncheckedException {
    private NexalithicRouteException(String message) {
        super(message);
    }

    private NexalithicRouteException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NexalithicRouteException wildcardAddress() {
        return new NexalithicRouteException("Wildcard address cannot be advertised to a client");
    }

    public static NexalithicRouteException invalidPort(int port) {
        return new NexalithicRouteException("Invalid route port: " + port);
    }

    public static NexalithicRouteException invalidCidr(String cidr) {
        return new NexalithicRouteException("Invalid CIDR: " + cidr);
    }

    public static NexalithicRouteException invalidCidrAddress(String cidr, Throwable cause) {
        return new NexalithicRouteException("Invalid CIDR address: " + cidr, cause);
    }

    public static NexalithicRouteException invalidCidrPrefix(String cidr) {
        return new NexalithicRouteException("Invalid CIDR prefix: " + cidr);
    }

    public static NexalithicRouteException invalidCidrPrefix(String cidr, Throwable cause) {
        return new NexalithicRouteException("Invalid CIDR prefix: " + cidr, cause);
    }
}
