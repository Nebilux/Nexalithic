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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * TCP 传输。拥有底层 SocketChannel，直接在 LoopBuffer 与 Socket 之间传递字节。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class TCP_Transport extends AbstractTransport<SocketChannel> {
    public TCP_Transport(SocketChannel channel) throws IOException {
        super(channel, (InetSocketAddress) channel.getRemoteAddress());
    }

    @Override
    public long read(LoopBuffer dst, int limit) throws IOException {
        return dst.readFromChannel(channel, limit);
    }

    @Override
    public long write(LoopBuffer src, int limit) throws IOException {
        return src.writeToChannel(channel, limit);
    }
}
