package com.thezeroer.nexalithic.core.infra.recyclable;

/**
 * 目标静态可回收包装器
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/19
 */
public abstract class TargetStaticRecyclableWrapper<T, W extends TargetStaticRecyclableWrapper<T, W>> extends GenericWrapperPool.AbstractRecyclableWrapper<T, W> {
    protected final T target;

    public TargetStaticRecyclableWrapper(GenericWrapperPool<T, W> owner, T target) {
        super(owner);
        this.target = target;
    }

    @Override
    public final T unwrap() {
        return target;
    }

    @Override
    protected final void onRecycle() {
        onReset();
    }
}
