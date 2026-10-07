package com.nebilux.nexalithic.server.session;

import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.codec.AssemblerFactory;
import com.nebilux.nexalithic.core.io.codec.FragmenterFactory;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.session.*;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.io.session.business.BusinessLoop;
import com.nebilux.nexalithic.server.io.session.signaling.SignalingLoop;
import com.nebilux.nexalithic.server.session.access.ChannelAccessCoordinator;

/**
 * 服务器会话
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ServerSession extends NexalithicSession<ServerSession> {
    private final ServiceUnit serviceUnit;
    private final ChannelAccessCoordinator channelAccessCoordinator;
    private volatile SessionAttachment attachment;

    public ServerSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey,
                         ChannelFactory factory, TaskScheduler scheduler, ServiceUnit serviceUnit,
                         ChannelAccessCoordinator channelAccessCoordinator) {
        super(sessionKey, signalingSecretKey, businessSecretKey, factory, scheduler);
        this.serviceUnit = serviceUnit;
        this.channelAccessCoordinator = channelAccessCoordinator;
    }

    public void attach(SessionAttachment attachment) {
        this.attachment = attachment;
    }
    @SuppressWarnings("unchecked")
    public <T extends SessionAttachment> T attachment()  {
        return (T) attachment;
    }

    public SignalingLoop getSignalingLoop() {
        return (SignalingLoop) getSignalingChannel().getOwnerLoop();
    }
    public BusinessLoop getBusinessLoop() {
        return (BusinessLoop) getBusinessChannel().getOwnerLoop();
    }

    public ServiceUnit getServiceUnit() {
        return serviceUnit;
    }

    @Override
    protected boolean requestChannelAccess(SessionChannel<?, ServerSession> channel) {
        return channelAccessCoordinator.issue(
                this, NexalithicChannel.Kind.Packet_Business, signalingChannel.getRemoteAddress().getAddress()
        ) == ChannelAccessCoordinator.Result.ISSUED;
    }

    @Override
    protected void onClose() {
        if (attachment != null) {
            attachment.clear();
            attachment = null;
        }
    }

    /**
     * 服务器通道工厂
     *
     * @author Reonvia
     * @since 0.1.0
     */
    public static class ChannelFactory implements SessionChannelFactory<ServerSession> {
        private final ServiceUnit serviceUnit;
        private final FragmenterFactory fragmenterFactory;
        private final AssemblerFactory assemblerFactory;

        public ChannelFactory(NexalithicBuilderContext context, ServiceUnit serviceUnit) {
            this.serviceUnit = serviceUnit;
            this.fragmenterFactory = new FragmenterFactory(context, NexalithicServer.MODULES);
            this.assemblerFactory = new AssemblerFactory(context, NexalithicServer.MODULES, NexalithicEndpoint.Type.SERVER);
        }

        @Override
        public SessionChannel<SignalingPacket, ServerSession> createSignalingChannel(ServerSession session, SecretKeyContext context) {
            return new SessionChannel<>(NexalithicChannel.Kind.Packet_Signaling, serviceUnit.getSignalingLoop(), session,
                    fragmenterFactory.createSignaling(), assemblerFactory.createSignaling(), context
            );
        }

        @Override
        public SessionChannel<BusinessPacket, ServerSession> createBusinessChannel(ServerSession session, SecretKeyContext context) {
            return new SessionChannel<>(NexalithicChannel.Kind.Packet_Business, serviceUnit.selectBusinessLoop(), session,
                    fragmenterFactory.createBusiness(session), assemblerFactory.createBusiness(session), context
            );
        }
    }
}
