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

package com.nebilux.nexalithic.core.connection.transport;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Nexalithic 通道的数据传输接口。
 *
 * <p>在 {@link LoopBuffer} 与对端之间传递数据。实现可以提供连续字节流或保留消息边界的传输语义。</p>
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface NexalithicTransport extends Closeable {
    /**
     * 将已可交付的数据写入目标缓冲区。
     *
     * <p>返回正数时，目标缓冲区的写入位置应推进相同的字节数。返回 {@code 0} 表示
     * 当前没有进展，例如暂时没有可交付数据或目标缓冲区没有可写空间。
     * 保留消息边界的实现不得向目标缓冲区交付不完整的消息。</p>
     *
     * @param dst 接收数据的缓冲区
     * @return 已写入的字节数；传输的输入端正常结束且没有剩余数据时返回 {@code -1}
     * @throws IOException 读取或传输处理失败时
     */
    long read(LoopBuffer dst) throws IOException;

    /**
     * 从源缓冲区接受待发送的数据。
     *
     * <p>返回正数时，源缓冲区的读取位置应推进相同的字节数；返回 {@code 0} 表示
     * 当前没有进展，例如没有可读数据或传输暂时无法接受数据。返回值只表示本地已接受
     * 的字节数，不表示数据已发出、到达或被对端处理。若发送将在方法返回后继续，
     * 实现必须自行持有已接受的数据，不得依赖调用方继续保留源缓冲区。
     * 保留消息边界的实现应将源缓冲区当前可读内容视为一条完整消息，整条接受或不接受。</p>
     *
     * @param src 包含待发送数据的缓冲区
     * @return 已接受的字节数
     * @throws IOException 写入或传输处理失败时
     */
    long write(LoopBuffer src) throws IOException;

    /**
     * 关闭传输并释放其持有的资源。
     *
     * <p>关闭不保证尚未确认的数据已经送达对端。</p>
     *
     * @throws IOException 释放资源失败时
     */
    @Override
    void close() throws IOException;
}
