package com.nebilux.nexalithic.core.infra.executor;

import java.util.concurrent.ThreadFactory;

/**
 * 泛型线程工厂，用于产出特定类型的执行线程
 *
 * @author Reonvia
 * @since 0.1.0
 */
@FunctionalInterface
public interface TypedThreadFactory<TH extends Thread> extends ThreadFactory {
    @Override
    TH newThread(Runnable r); // 协变返回类型：将 Thread 改为 TH
}