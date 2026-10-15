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
import com.nebilux.nexalithic.core.connection.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.connection.channel.SessionChannel;
import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.connection.session.SessionKey;
import com.nebilux.nexalithic.core.io.codec.AssemblerFactory;
import com.nebilux.nexalithic.core.io.codec.CodecResources;
import com.nebilux.nexalithic.core.io.codec.FragmenterFactory;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.security.SecretKeyContexts;

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
        private final FragmenterFactory fragmenterFactory;
        private final AssemblerFactory assemblerFactory;

        public ChannelFactory(ClientSessionLoop loop, CodecResources resources) {
            this.fragmenterFactory = resources.fragmenters();
            this.assemblerFactory = resources.assemblers();
            this.loop = loop;
        }

        @Override
        public SessionChannel<?, ClientSession> create(NexalithicChannel.Kind kind, ClientSession session, SecretKeyContext secretKeyContext) {
            return switch (kind) {
                case Packet_Signaling -> new SessionChannel<>(kind, loop, session,
                        fragmenterFactory.createSignaling(), assemblerFactory.createSignaling(), secretKeyContext);
                case Packet_Business -> new SessionChannel<>(kind, loop, session,
                        fragmenterFactory.createBusiness(session), assemblerFactory.createBusiness(session), secretKeyContext);
                case Stream_Media -> new SessionChannel<>(kind, loop, session,
                        fragmenterFactory.createMedia(), assemblerFactory.createMedia(), secretKeyContext);
                case Stream_File -> new SessionChannel<>(kind, loop, session,
                        fragmenterFactory.createFile(), assemblerFactory.createFile(), secretKeyContext);
            };
        }
    }
}
