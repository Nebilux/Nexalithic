package com.thezeroer.nexalithic.core.infra.recyclable;

/**
 * 包装器池 (Wrapper Pool)
 * <p>负责管理 {@link RecyclableWrapper} 实例的生命周期。</p>
 *
 * @param <W> 池所管理的具体包装器类型，必须是自引用泛型。
 * @author tbrtz647@outlook.com
 * @since 2026/02/11
 * @version 1.0.0
 */
public interface WrapperPool<W extends RecyclableWrapper<?>> {
    /**
     * 从池中获取一个可用的包装器。
     * <p>如果池为空，实现类应根据策略创建新实例或阻塞等待。</p>
     * @return 准备就绪的包装器实例
     */
    W acquire();

    /**
     * 执行池预热
     * @param prefillRatio 预热比例 (0.0 ~ 1.0)。
     * 例如 0.5 表示启动时填充 50% 的容量。
     * @throws IllegalArgumentException 如果比例不在有效范围内
     */
    WrapperPool<W> warmUp(double prefillRatio);
}
