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
 * 用户从输出流取出完整块。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface StreamDrainer<C extends BusinessStream.Chunk> {
    /** 返回下一个完整块；目前没有块时返回 null。 */
    C pollChunk();

    /** 已收到结束标记且队列已经取空时返回 true。 */
    boolean isFinished();

    /**
     * 设置新块可供读取时的通知。实现必须在锁外调用回调；传入 null 清除回调。
     */
    void onAvailable(Runnable callback);
}
