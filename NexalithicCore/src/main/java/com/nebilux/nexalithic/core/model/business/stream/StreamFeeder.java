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

package com.nebilux.nexalithic.core.model.business.stream;

/**
 * 用户向输入流提交完整块。
 *
 * @param <C> 块类型
 * @since 0.5.0
 */
public interface StreamFeeder<C extends BusinessStream.Chunk> {
    enum OfferResult {
        /** 新块已排队；满队列策略可能同时淘汰了旧块。 */
        ENQUEUED,
        /** 队列暂时没有容量，调用方可保留该块并稍后重试。 */
        RETRY,
        /** 满队列策略丢弃了新块，不应将其作为同一次提交重试。 */
        DROPPED
    }

    /**
     * 提交块。结果区分已入队、可重试的背压与策略性丢弃。
     */
    OfferResult offerChunk(C chunk);

    /** 宣告不再产生新块；已排队的块仍可被取走。 */
    void finish();

    /**
     * 设置队列恢复容量时的通知。{@link OfferResult#RETRY} 之后可注册回调并重试原块；
     * 若注册时容量已恢复，回调可立即执行。回调不保证指定块仍可提交，
     * 因此仍须检查 {@code offerChunk} 的结果。
     * 实现必须在锁外调用回调；传入 {@code null} 清除回调。
     */
    void onAvailable(Runnable callback);
}
