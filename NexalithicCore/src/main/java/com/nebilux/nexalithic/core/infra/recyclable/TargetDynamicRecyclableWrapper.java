/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.infra.recyclable;

/**
 * 目标动态可回收包装器
 *
 * @author Reonvia
 * @since 0.2.0
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
