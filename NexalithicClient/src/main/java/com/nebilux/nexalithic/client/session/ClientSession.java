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

package com.nebilux.nexalithic.client.session;

import com.nebilux.nexalithic.client.io.session.ClientSessionLoop;
import com.nebilux.nexalithic.core.connection.channel.NexalithicChannel.Kind;
import com.nebilux.nexalithic.core.connection.channel.SessionBusinessChannel;
import com.nebilux.nexalithic.core.connection.channel.SessionChannel;
import com.nebilux.nexalithic.core.connection.channel.SessionSignalingChannel;
import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.connection.session.SessionKey;
import com.nebilux.nexalithic.core.io.codec.CodecResources;
import com.nebilux.nexalithic.core.io.codec.CodecSupplier;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.security.SecretKeyContexts;

import java.util.EnumMap;

/**
 * 客户端会话
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClientSession extends AbstractSession<ClientSession> {
    private final SessionManager sessionManager;

    public ClientSession(SessionKey sessionKey, SecretKeyContexts secretKeyContexts,
                         ChannelFactory factory, TaskScheduler scheduler, SessionManager sessionManager) {
        super(sessionKey, secretKeyContexts, factory, scheduler);
        this.sessionManager = sessionManager;
    }

    @Override
    protected boolean requestChannelAccess(SessionChannel<?, ClientSession> channel) {
        return sessionManager.requestChannelAccess(channel);
    }

    /**
     * 客户端通道工厂
     *
     * @author Reonvia
     * @since 0.1.0
     */
    public static class ChannelFactory implements AbstractSession.ChannelFactory<ClientSession> {
        private final ClientSessionLoop loop;
        private final CodecSupplier codecSupplier;

        public ChannelFactory(ClientSessionLoop loop, CodecResources resources) {
            this.codecSupplier = resources.codecFactory();
            this.loop = loop;
        }

        @Override
        public ChannelGroup<ClientSession> create(ClientSession session, SecretKeyContexts secretKeyContexts) {
            SessionSignalingChannel<ClientSession> signaling = new SessionSignalingChannel<>(loop, session,
                    codecSupplier.createSignalingPacketFragmenter(), codecSupplier.createSignalingPacketAssembler(),
                    secretKeyContexts.get(Kind.Signaling_Packet));
            EnumMap<Kind, SessionBusinessChannel<?, ClientSession>> business = new EnumMap<>(Kind.class);
            business.put(Kind.Business_Packet, new SessionBusinessChannel<>(Kind.Business_Packet, loop, session,
                    codecSupplier.createBusinessPacketFragmenter(session), codecSupplier.createBusinessPacketAssembler(session),
                    secretKeyContexts.get(Kind.Business_Packet)));
            business.put(Kind.Business_Stream, new SessionBusinessChannel<>(Kind.Business_Stream, loop, session,
                    codecSupplier.createBusinessStreamFragmenter(session), codecSupplier.createBusinessStreamAssembler(session),
                    secretKeyContexts.get(Kind.Business_Stream)));
            return new ChannelGroup<>(signaling, business);
        }
    }
}
