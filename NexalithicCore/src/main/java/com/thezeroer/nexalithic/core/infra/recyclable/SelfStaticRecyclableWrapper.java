package com.thezeroer.nexalithic.core.infra.recyclable;

/**
 * 自静态可回收包装器
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/19
 */
public abstract class SelfStaticRecyclableWrapper<W extends SelfStaticRecyclableWrapper<W>> extends GenericWrapperPool.AbstractRecyclableWrapper<W, W>{
    public SelfStaticRecyclableWrapper(GenericWrapperPool<W, W> owner) {
        super(owner);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final W unwrap() {
        return (W) this;
    }

    @Override
    protected final void onRecycle() {
        onReset();
    }
}
