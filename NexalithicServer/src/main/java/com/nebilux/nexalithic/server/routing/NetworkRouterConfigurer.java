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

import com.nebilux.nexalithic.core.builder.NexalithicConfigurer;

/**
 * 网络路由器配置器。
 *
 * <p>用于集中配置 CIDR 路由和默认路由。配置目标同时也是服务器运行期间使用的
 * {@link NetworkRouter} 实例，因此构建阶段写入的路由会直接成为初始路由表。</p>
 *
 * @author Reonvia
 * @since 0.3.0
 */
@FunctionalInterface
public interface NetworkRouterConfigurer extends NexalithicConfigurer<NetworkRouter, Void> {
    /** 配置网络路由器。 */
    void configure(NetworkRouter router);

    @Override
    default void configure(NetworkRouter router, Void helper) {
        configure(router);
    }
}
