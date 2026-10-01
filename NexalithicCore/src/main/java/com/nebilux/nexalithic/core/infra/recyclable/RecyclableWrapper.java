package com.nebilux.nexalithic.core.infra.recyclable;

/**
 * 池化资源的可回收包装器 (Recyclable Wrapper)
 *
 * @param <T> 被包装的底层资源类型
 * @author Reonvia
 * @since 0.1.0
 */
public interface RecyclableWrapper<T> {
    /**
     * 显式回收包装器。
     * <p>根据实现类不同，可能执行不同的重置逻辑（如 {@code target.clear()} 或 {@code target = null}）。</p>
     * <p><b>幂等性要求</b></p>
     */
    void recycle();

    /**
     * 提取内部对象。
     * <p><b>约束：</b> 严禁将此返回值逃逸（Escape）到当前线程栈之外，除非明确了解其生命周期。</p>
     * @return 内部对象引用
     */
    T unwrap();
}
