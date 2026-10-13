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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.util.Objects;

/**
 * 抽象数据传输
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class AbstractTransport<C extends SelectableChannel> implements NexalithicTransport {
    protected final C channel;
    private final InetSocketAddress remoteAddress;

    protected AbstractTransport(C channel, InetSocketAddress remoteAddress) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
    }

    public final C selectableChannel() {
        return channel;
    }

    public final InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
