package com.thezeroer.nexalithic.core.infra.recyclable;

/**
 * <h2>池化存储抽象接口 (Pool Storage)</h2>
 *
 * @param <T> 存储的资源类型
 * @author tbrtz647@outlook.com
 * @since 2026/03/11
 * @version 1.0.0
 */
public interface PoolStorage<T> {
    /**
     * 将资源尝试归还至存储。
     * @param t 待回收的资源实例
     * @return {@code true} 如果归还成功；{@code false} 如果存储已满（触发溢出逻辑）
     */
    boolean offer(T t);

    /**
     * 从存储中获取一个可用资源。
     * @return 资源实例，若存储为空则返回 {@code null}
     */
    T poll();

    /**
     * 获取该存储单元的逻辑容量上限。
     * <p>常用于监控指标统计或作为溢出控制的阈值基础。</p>
     * @return 存储允许的最大容量
     */
    int capacity();

    int size();
}
