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

package com.nebilux.nexalithic.core.model.stream.file;

import com.nebilux.nexalithic.core.model.stream.AbstractStream;
import com.nebilux.nexalithic.core.model.stream.ChunkReceiver;
import com.nebilux.nexalithic.core.model.stream.ChunkSender;
import com.nebilux.nexalithic.core.model.stream.StreamModelProvider;

/**
 * 按序传递文件块的公开视图。应用从文件读取并通过 {@link #sender()} 提交块，
 * 或通过 {@link #receiver()} 取出块并写入文件。流只负责块的缓冲和顺序校验。
 * 首块的序号和偏移均为 0，后续块必须连续；队列满时提交返回 {@code false}，
 * 调用方保留该块并稍后重试。双向流为两个方向分别保存块队列。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class FileStream extends AbstractStream<FileChunk> {
    protected FileStream(Direction direction) {
        super(Type.File, direction);
    }

    /** 打开指定方向的文件流，默认每个方向的队列容量为 64 块。 */
    public static FileStream open(Direction direction) {
        return StreamModelProvider.provider().openFileStream(direction);
    }

    /** 在首次取得 sender/receiver 或绑定编解码器之前配置每个方向的队列容量。 */
    public abstract FileStream queueCapacity(int queueCapacity);

    @Override
    public abstract ChunkSender<FileChunk> sender();

    @Override
    public abstract ChunkReceiver<FileChunk> receiver();
}
