package com.nebilux.nexalithic.core.infra.loadbalance;

/**
 * 可负载均衡的
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/02/21
 */
public interface LoadBalanceable {
    /**
     * 获取当前组件的“负载分值”
     * 分值越低，代表越空闲，越应该被选中
     */
    long getLoadScore();
}