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

package com.nebilux.nexalithic.core.infra.concurrent.atomic;

import java.util.Objects;
import java.util.Queue;
import java.util.function.Predicate;

/**
 * 只能从接收新元素转换为拒绝新元素的状态队列。
 *
 * <p>初始状态为接收。调用 {@link #close()} 后，新的 {@link #offer(Object)}
 * 会被拒绝；关闭会等待此前已通过准入检查的入队操作结束，但不消费或丢弃
 * 队列中的元素。关闭不可逆，也不会阻止 {@link #poll()} 和 {@link #drain(java.util.function.Consumer)}。</p>
 *
 * <p>可选的准入条件仅在队列仍接收新元素时执行。该条件不得同步调用本队列的
 * {@code close()}，否则关闭会等待当前入队操作自身结束。底层队列仍须符合
 * 调用方所需的并发模型。</p>
 *
 * @param <E> 队列元素类型
 * @author Reonvia
 * @since 0.3.0
 */
public final class AtomicAdmissionQueue<E> extends AtomicStateQueue<E, AtomicAdmissionQueue.AdmissionState> {
    public enum AdmissionState {
        OPEN,
        CLOSED
    }

    public AtomicAdmissionQueue(Queue<E> delegateQueue) {
        this(delegateQueue, element -> true);
    }

    public AtomicAdmissionQueue(Queue<E> delegateQueue, Predicate<? super E> admissionPolicy) {
        this(delegateQueue, new StateHandle<>(AdmissionState.OPEN), Objects.requireNonNull(admissionPolicy, "admissionPolicy"));
    }

    private AtomicAdmissionQueue(Queue<E> delegateQueue, StateHandle<AdmissionState> admissionState, Predicate<? super E> admissionPolicy) {
        super(delegateQueue, admissionState, (state, element) -> state == AdmissionState.OPEN && admissionPolicy.test(element));
    }

    /**
     * 判断是否仍接受新元素。并发关闭时该结果只表示调用瞬间的观察值。
     */
    public boolean isAccepting() {
        return stateHandle.get() == AdmissionState.OPEN;
    }

    /**
     * 永久拒绝新元素，并等待此前已通过准入检查的入队操作退出。
     *
     * @return 本次调用执行了状态转换时返回 {@code true}；已经关闭时返回 {@code false}
     */
    public boolean close() {
        return stateHandle.compareAndSet(AdmissionState.OPEN, AdmissionState.CLOSED);
    }
}
