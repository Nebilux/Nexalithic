package com.thezeroer.nexalithic.core.infra.recyclable;

/**
 * 目标动态可回收包装器
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/19
 */
public abstract class TargetDynamicRecyclableWrapper<T, W extends TargetDynamicRecyclableWrapper<T, W>> extends GenericWrapperPool.AbstractRecyclableWrapper<T, W> {
    protected volatile T target;

    public TargetDynamicRecyclableWrapper(GenericWrapperPool<T, W> owner) {
        super(owner);
    }

    @SuppressWarnings("unchecked")
    public final W wrap(T target) {
        if (!isActive()) {
            throw new IllegalStateException("Cannot wrap target while state is " + state());
        }
        if (this.target != null) {
            throw new IllegalStateException("Wrapper already contains a target");
        }
        onWrap(target);
        this.target = target;
        return (W)this;
    }

    @Override
    public final T unwrap() {
        return target;
    }

    protected void onWrap(T target) {}

    @Override
    protected final void onRecycle() {
        try {
            onReset();
        } finally {
            target = null;
        }
    }
}
