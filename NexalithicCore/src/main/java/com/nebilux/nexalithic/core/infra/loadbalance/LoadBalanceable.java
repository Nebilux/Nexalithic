package com.nebilux.nexalithic.core.infra.loadbalance;

/**
 * 可负载均衡的
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface LoadBalanceable {
    /**
     * 获取当前组件的“负载分值”
     * 分值越低，代表越空闲，越应该被选中
     */
    long getLoadScore();
}