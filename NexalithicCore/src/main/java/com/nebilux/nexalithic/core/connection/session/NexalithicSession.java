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

package com.nebilux.nexalithic.core.connection.session;

import com.nebilux.nexalithic.core.connection.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.connection.channel.SessionChannel;
import com.nebilux.nexalithic.core.model.NexalithicModel;
import com.nebilux.nexalithic.core.model.signaling.packet.SignalingPacket;

/**
 * nexalithic会话
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface NexalithicSession {
    /** Session 的逻辑生命周期。 */
    enum State {
        /** Session 逻辑身份有效，可以处理数据和建立附属 Channel。 */
        ACTIVE,

        /** 已经领取停用权，正在断开底层 Channel 并执行收尾。 */
        DEACTIVATING,

        /** Session 逻辑身份已经失效，不再允许任何操作。 */
        INACTIVE
    }

    SessionChannel<SignalingPacket, ?> getSignalingChannel();

    /** 调用方指定的模型类型必须与 {@code kind} 对应。 */
    <M extends NexalithicModel> SessionChannel<M, ?> getChannel(NexalithicChannel.Kind kind);

    SessionKey getSessionKey();

    State getState();

    void close();
}
