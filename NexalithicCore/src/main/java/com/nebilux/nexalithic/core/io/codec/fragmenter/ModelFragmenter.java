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

package com.nebilux.nexalithic.core.io.codec.fragmenter;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.NexalithicModel;

import java.io.IOException;

/**
 * 将模型转换为待发送字节的通用接口。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface ModelFragmenter<M extends NexalithicModel> {
    boolean feed(M model);
    boolean drain(LoopBuffer target) throws IOException;

    /** 已提交的模型及其后续数据均已处理完毕时返回 true。等待后续块的 Stream 不算完成。 */
    boolean isEmpty();

    /**
     * 当前有可立即写出的数据时返回 true。Stream 暂时没有块时应返回 false，
     * 并在新块可用时重新启用所属通道的写兴趣。
     */
    default boolean isReady() {
        return !isEmpty();
    }

    void clear();
}
