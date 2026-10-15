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

package com.nebilux.nexalithic.core.model.stream;

/**
 * 用户向输入流提交完整块；编解码器通过流的内部接口取走块。
 *
 * @param <C> 块类型
 * @since 0.5.0
 */
public interface ChunkSender<C extends AbstractChunk> {
    /**
     * 提交块。返回 false 时调用方仍持有该块，可稍后重试。
     * 文件实现不会丢弃已排队的块；媒体实现可以通过丢弃更旧的排队块接受新块。
     */
    boolean offerChunk(C chunk);

    /** 宣告不再产生新块；已排队的块仍可被取走。 */
    void finish();
}
