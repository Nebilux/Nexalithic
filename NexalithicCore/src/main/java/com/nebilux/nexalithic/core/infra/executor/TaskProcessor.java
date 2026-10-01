package com.nebilux.nexalithic.core.infra.executor;

/**
 * 任务处理器
 *
 * @author Reonvia
 * @since 0.1.0
 */
@FunctionalInterface
public interface TaskProcessor<T, TH extends Thread> {
    void process(T target, TH thread);
}