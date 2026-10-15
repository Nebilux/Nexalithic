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

package com.nebilux.nexalithic.core.model.stream.media;

import com.nebilux.nexalithic.core.model.stream.AbstractStream;
import com.nebilux.nexalithic.core.model.stream.ChunkReceiver;
import com.nebilux.nexalithic.core.model.stream.ChunkSender;
import com.nebilux.nexalithic.core.model.stream.StreamModelProvider;

/**
 * 按块数限制容量的媒体流的公开视图。双向流为输入和输出分别保存块队列。
 * 块可以有序号缺口，队列满时保留较新的块。
 * 不负责媒体采集、播放、编码或传输层重传。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class MediaStream extends AbstractStream<MediaChunk> {
    protected MediaStream(Direction direction) {
        super(Type.Media, direction);
    }

    /** 打开指定方向的媒体流，默认队列容量为 64 块。 */
    public static MediaStream open(Direction direction) {
        return StreamModelProvider.provider().openMediaStream(direction);
    }

    /** 在首次取得 sender/receiver 或绑定编解码器之前配置每个方向的队列容量。 */
    public abstract MediaStream queueCapacity(int queueCapacity);

    @Override
    public abstract ChunkSender<MediaChunk> sender();

    @Override
    public abstract ChunkReceiver<MediaChunk> receiver();

    /** 因队列满或到达过晚而被丢弃的块数。 */
    public abstract long droppedChunks();
}
