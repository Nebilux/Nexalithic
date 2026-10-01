package com.nebilux.nexalithic.core.infra.loadbalance;

/**
 * 负载均衡器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface LoadBalancer<K, V extends LoadBalanceable> {
    V select(K k);

    V[] all();

    default int size() {
        return all().length;
    }
}
