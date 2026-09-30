package com.nebilux.nexalithic.core.infra.loadbalance;

/**
 * 负载均衡器
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/02/21
 */
public interface LoadBalancer<K, V extends LoadBalanceable> {
    V select(K k);

    V[] all();

    default int size() {
        return all().length;
    }
}
