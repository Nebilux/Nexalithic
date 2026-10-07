package com.nebilux.nexalithic.server.routing;

import com.nebilux.nexalithic.core.builder.NexalithicConfigurer;

/**
 * 网络路由器配置器。
 *
 * <p>用于集中配置 CIDR 路由和默认路由。配置目标同时也是服务器运行期间使用的
 * {@link NetworkRouter} 实例，因此构建阶段写入的路由会直接成为初始路由表。</p>
 *
 * @author Reonvia
 * @since 0.2.2
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
