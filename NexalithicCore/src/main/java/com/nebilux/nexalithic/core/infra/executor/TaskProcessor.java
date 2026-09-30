package com.nebilux.nexalithic.core.infra.executor;

/**
 * 任务处理器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/09
 * @version 1.0.0
 */
@FunctionalInterface
public interface TaskProcessor<T, TH extends Thread> {
    void process(T target, TH thread);
}