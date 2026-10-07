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

package com.nebilux.nexalithic.server.messaging;

import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.SessionAttachment;
import com.nebilux.nexalithic.server.session.ServerSession;
import com.nebilux.nexalithic.server.session.SessionRegistry;

import java.net.InetAddress;

/**
 * 服务端处理器上下文
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ServerHandlerContext extends HandlerContext<ServerSession> {
    private final SessionRegistry sessionRegistry;

    public ServerHandlerContext(SessionRegistry sessionRegistry) {
        this.sessionRegistry = sessionRegistry;
    }

    public boolean push(String sessionName, BusinessPacket packet) {
        ServerSession session = sessionRegistry.getSession(sessionName);
        if (session == null) {
            return false;
        }
        return session.pushBusinessPacket(packet);
    }

    public void forceSetSessionName(String sessionName) {
        ServerSession existing = sessionRegistry.forceSetSessionName(sessionName, session);
        if (existing != null) {
            existing.close();
        }
    }
    public boolean trySetSessionName(String sessionName) {
        return sessionRegistry.trySetSessionName(sessionName, session);
    }
    public String getSessionName() {
        return session.getSessionName();
    }

    public void attach(SessionAttachment attachment) {
        session.attach(attachment);
    }
    public <T extends SessionAttachment> T attachment()  {
        return session.attachment();
    }

    public InetAddress getRemoteAddress() {
        return session.getSignalingChannel().getRemoteAddress().getAddress();
    }

    public void broadcastToOthers(BusinessPacket packet) {
        packet.seal();
        String currentSessionName = session.getSessionName();
        sessionRegistry.forEachNamedSession(s -> {
            if (!s.getSessionName().equals(currentSessionName)) {
                s.pushBusinessPacket(packet.duplicate());
            }
        });
    }

    public static class Recyclable extends HandlerContext.Recyclable<
            ServerSession,
            ServerHandlerContext,
            Recyclable
        > {
        public Recyclable(GenericWrapperPool<ServerHandlerContext, Recyclable> owner, ServerHandlerContext target) {
            super(owner, target);
        }
    }
}
