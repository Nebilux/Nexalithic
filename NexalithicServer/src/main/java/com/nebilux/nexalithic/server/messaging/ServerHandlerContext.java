package com.nebilux.nexalithic.server.messaging;

import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.SessionAttachment;
import com.nebilux.nexalithic.server.session.ServerSession;
import com.nebilux.nexalithic.server.manager.SessionsManager;

import java.net.InetAddress;

/**
 * 服务端处理器上下文
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ServerHandlerContext extends HandlerContext<ServerSession> {
    private final SessionsManager sessionsManager;

    public ServerHandlerContext(SessionsManager sessionsManager) {
        this.sessionsManager = sessionsManager;
    }

    public boolean push(String sessionName, BusinessPacket packet) {
        ServerSession session = sessionsManager.getSession(sessionName);
        if (session == null) {
            return false;
        }
        return session.pushBusinessPacket(packet);
    }

    public void forceSetSessionName(String sessionName) {
        ServerSession existing = sessionsManager.forceSetSessionName(sessionName, session);
        if (existing != null) {
            existing.close();
        }
    }
    public boolean trySetSessionName(String sessionName) {
        return sessionsManager.trySetSessionName(sessionName, session);
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
        sessionsManager.forEachNamedSession(s -> {
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
