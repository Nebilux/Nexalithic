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

import com.nebilux.nexalithic.core.model.AbstractModel;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.stream.chunk.AbstractChunk;

/**
 * 抽象流
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class AbstractStream<C extends AbstractChunk> implements AbstractModel {
    public enum StreamType {
        /** 媒体 */ Media,
        /** 文件 */ File
    }

    /**
     * 获取流类型
     *
     * @return {@link AbstractPacket.PacketType }
     */
    public abstract StreamType streamType();
    public final ModelType modelType() {
        return ModelType.Stream;
    }
}
