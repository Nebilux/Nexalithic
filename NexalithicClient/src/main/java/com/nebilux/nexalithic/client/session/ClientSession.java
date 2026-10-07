package com.nebilux.nexalithic.client.session;

import com.nebilux.nexalithic.client.NexalithicClient;
import com.nebilux.nexalithic.client.io.session.ClientSessionLoop;
import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.codec.AssemblerFactory;
import com.nebilux.nexalithic.core.io.codec.FragmenterFactory;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import com.nebilux.nexalithic.core.session.SessionChannel;
import com.nebilux.nexalithic.core.session.SessionChannelFactory;
import com.nebilux.nexalithic.core.session.SessionKey;

/**
 * 客户端会话
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClientSession extends NexalithicSession<ClientSession> {
    private final SessionManager sessionManager;

    public ClientSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey,
                         ChannelFactory factory, TaskScheduler scheduler, SessionManager sessionManager) {
        super(sessionKey, signalingSecretKey, businessSecretKey, factory, scheduler);
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
    public static class ChannelFactory implements SessionChannelFactory<ClientSession> {
        private final ClientSessionLoop loop;
        private final FragmenterFactory fragmenterFactory;
        private final AssemblerFactory assemblerFactory;

        public ChannelFactory(NexalithicBuilderContext context, ClientSessionLoop loop) {
            this.fragmenterFactory = new FragmenterFactory(context, NexalithicClient.MODULES);
            this.assemblerFactory = new AssemblerFactory(context, NexalithicClient.MODULES, NexalithicEndpoint.Type.CLIENT);
            this.loop = loop;
        }

        @Override
        public SessionChannel<SignalingPacket, ClientSession> createSignalingChannel(ClientSession session, SecretKeyContext context) {
            return new SessionChannel<>(NexalithicChannel.Kind.Packet_Signaling, loop, session,
                    fragmenterFactory.createSignaling(), assemblerFactory.createSignaling(), context
            );
        }

        @Override
        public SessionChannel<BusinessPacket, ClientSession> createBusinessChannel(ClientSession session, SecretKeyContext context) {
            return new SessionChannel<>(NexalithicChannel.Kind.Packet_Business, loop, session,
                    fragmenterFactory.createBusiness(session), assemblerFactory.createBusiness(session), context
            );
        }
    }
}
