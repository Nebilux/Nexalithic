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

package com.nebilux.nexalithic.core.infra.timer;

/**
 * 定时器协调器可读取的调度上下文。
 *
 * <p>上下文不是可长期持有的注册句柄，也不提供取消或续期操作。上下文仅在当前
 * {@link TimerCoordinator} 方法调用期间有效，不得缓存、跨线程传递或异步访问。</p>
 *
 * @param <T> 被调度的目标类型
 * @author Reonvia
 * @since 0.2.0
 */
public interface TimerContext<T> {

    /**
     * 获取本次调度携带的不透明标识。
     *
     * <p>该值由调用方在 {@code TimeWheel.schedule(T, long)} 或对应重载中传入，
     * 时间轮只负责原样保存和返回，不解释其业务含义。调用方可将其用作版本号或租约标识，
     * 通过与目标当前标识比较来拒绝已经过期的旧注册。未显式提供时，该值为 {@code 0}。</p>
     *
     * @return 当前注册携带的标识
     */
    long targetStamp();

    /**
     * 获取本次调度关联的目标对象。
     *
     * <p>返回值与调用 {@code schedule} 时传入的目标相同。上下文本身不能被保存；
     * 是否可以在回调外持有目标对象，则由目标自身的生命周期规则决定。</p>
     *
     * @return 当前注册关联的目标对象
     */
    T target();
}
