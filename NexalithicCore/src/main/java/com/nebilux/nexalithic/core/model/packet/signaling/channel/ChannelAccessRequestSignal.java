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

package com.nebilux.nexalithic.core.model.packet.signaling.channel;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;

/**
 * 指定目标 Channel 类型的接入请求。
 *
 * @author Reonvia
 * @since 0.3.0
 */
public final class ChannelAccessRequestSignal extends ChannelAccessSignal {
    private static final int CONTENT_LENGTH = KIND_LENGTH;

    public ChannelAccessRequestSignal(NexalithicChannel.Kind kind) {
        super(Signal.ChannelAccess_Request, kind, CONTENT_LENGTH);
    }

    private ChannelAccessRequestSignal(LoopBuffer buffer, int contentLength) {
        super(Signal.ChannelAccess_Request, buffer, contentLength, CONTENT_LENGTH, CONTENT_LENGTH);
    }

    public static ChannelAccessRequestSignal fromBuffer(LoopBuffer buffer, int contentLength) {
        return new ChannelAccessRequestSignal(buffer, contentLength);
    }

    @Override
    protected void writePayload(LoopBuffer buffer) {}
}
