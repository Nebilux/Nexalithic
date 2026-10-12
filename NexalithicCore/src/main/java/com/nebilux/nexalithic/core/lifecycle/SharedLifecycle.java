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

package com.nebilux.nexalithic.core.lifecycle;

import com.nebilux.nexalithic.core.lifecycle.Lifecycle.ShutdownMode;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 按调用者身份共享一次性生命周期组件。
 * 首个调用者执行 {@link #start(Object)} 时启动底层组件，最后一个已启动的调用者
 * {@link #release(Object, ShutdownMode)} 退出时关闭它。
 *
 * <p>同一调用者重复启动不增加持有数，重复退出是无操作；未启动的调用者退出不触发关闭。
 * 任一已启动调用者选择 ABORT 时，最终关闭模式为 ABORT，但不提前中止其他调用者。
 * 首次启动与最后关闭并发时，由底层 {@link Lifecycle} 决定结果。
 * 非最后调用者的释放阶段只表示持有权已交还；要等待最终终止，请调用
 * {@link #whenTerminated()}。</p>
 *
 * @param <T> 被共享的生命周期组件
 *
 * @author Reonvia
 * @since 0.4.0
 */
public class SharedLifecycle<T extends Lifecycle> {
    private final T component;
    private final Object lock = new Object();
    private final Set<Object> callers = Collections.newSetFromMap(new IdentityHashMap<>());
    private Lifecycle.ShutdownMode shutdownMode = null;

    public SharedLifecycle(T component) {
        this.component = Objects.requireNonNull(component, "component");
    }

    /** 首次调用者启动底层组件；同一调用者重复调用不增加持有数。 */
    public CompletionStage<Void> start(Object caller) {
        Objects.requireNonNull(caller, "caller");
        boolean isEmpty;
        synchronized (lock) {
            if (callers.contains(caller)) {
                return component.whenStarted();
            }
            isEmpty = callers.isEmpty();
            if (isEmpty && shutdownMode != null) {
                throw new IllegalStateException("Shared lifecycle is closing");
            }
            callers.add(caller);
        }
        if (!isEmpty) {
            return component.whenStarted();
        }
        try {
            return Objects.requireNonNull(component.start(), "lifecycle start stage");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /**
     * 交还调用者的持有权。未启动或非最后调用者立即完成；最后调用者等待底层关闭。
     * 重复释放是无操作，后续传入的关闭模式不再生效。
     */
    public CompletionStage<Void> release(Object caller, ShutdownMode mode) {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(mode, "mode");
        synchronized (lock) {
            if (!callers.remove(caller)) {
                return CompletableFuture.completedFuture(null);
            }
            if (shutdownMode == null || mode == ShutdownMode.ABORT) {
                shutdownMode = mode;
            }
            if (!callers.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
        }
        try {
            return Objects.requireNonNull(component.shutdown(shutdownMode), "lifecycle shutdown stage");
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** 返回底层组件；其启动和关闭必须通过此控制器进行。 */
    public T component() {
        return component;
    }

    /** 观察底层组件的启动结果，不主动启动。 */
    public CompletionStage<Void> whenStarted() {
        return component.whenStarted();
    }

    /** 观察底层组件的终止结果，不主动关闭。 */
    public CompletionStage<Void> whenTerminated() {
        return component.whenTerminated();
    }

    /** 是否已有调用者执行过 start；供共享模块终止等待排除未使用的组件。 */
    public boolean wasStarted() {
        synchronized (lock) {
            return !callers.isEmpty() || shutdownMode != null;
        }
    }
}
