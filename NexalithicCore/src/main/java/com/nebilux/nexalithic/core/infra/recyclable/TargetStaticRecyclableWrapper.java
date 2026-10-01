package com.nebilux.nexalithic.core.infra.recyclable;

/**
 * 目标静态可回收包装器
 *
 * @author Reonvia
 * @since 0.2.0
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
