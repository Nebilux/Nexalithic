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

package com.nebilux.nexalithic.core.connection.channel;

import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.io.codec.assembler.ModelAssembler;
import com.nebilux.nexalithic.core.io.codec.fragmenter.ModelFragmenter;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.model.signaling.packet.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;

/**
 * 会话信令通道
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class SessionSignalingChannel<S extends AbstractSession<S>> extends SessionChannel<SignalingPacket, S> {
    public SessionSignalingChannel(SessionLoop<?, ? super SessionChannel<SignalingPacket, S>> ownerLoop, S ownerSession,
                                   ModelFragmenter<SignalingPacket> fragmenter, ModelAssembler<SignalingPacket> assembler, SecretKeyContext secretKeyContext) {
        super(Kind.Signaling_Packet, ownerLoop, ownerSession, fragmenter, assembler, secretKeyContext);
    }
}
