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

package com.nebilux.nexalithic.core.internal.model.stream;

import com.nebilux.nexalithic.core.model.business.stream.ChunkReceiver;
import com.nebilux.nexalithic.core.model.business.stream.ChunkSender;
import com.nebilux.nexalithic.core.model.business.stream.media.MediaChunk;
import com.nebilux.nexalithic.core.model.business.stream.media.MediaStream;

/**
 * 内置媒体流实现；块队列和编解码器入口不属于公开的 MediaStream API。
 *
 * @author Reonvia
 * @since 0.5.0
 */
final class MediaStreamImpl extends MediaStream {
    MediaStreamImpl(Direction direction, Way way, short... path) {
        super(direction, way, path);
    }

    @Override
    public MediaStream queueCapacity(int queueCapacity) {
        return null;
    }

    @Override
    public ChunkSender<MediaChunk> sender() {
        return null;
    }

    @Override
    public ChunkReceiver<MediaChunk> receiver() {
        return null;
    }

    @Override
    public long droppedChunks() {
        return 0;
    }
}
