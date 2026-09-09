package com.thezeroer.nexalithic.client.lifecycle.session;

import com.thezeroer.nexalithic.client.NexalithicClient;
import com.thezeroer.nexalithic.client.lifecycle.GeneralLoop;
import com.thezeroer.nexalithic.client.manager.NetworkRouter;
import com.thezeroer.nexalithic.core.NexalithicEndpoint;
import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.io.codec.AssemblerFactory;
import com.thezeroer.nexalithic.core.io.codec.FragmenterFactory;
import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.model.packet.signaling.BareSignal;
import com.thezeroer.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.session.NexalithicSession;
import com.thezeroer.nexalithic.core.session.SessionKey;
import com.thezeroer.nexalithic.core.session.channel.ChannelFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 客户端会话
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/09
 */
public class ClientSession extends NexalithicSession<
        ClientSession,
        ClientSessionChannel<SignalingPacket>,
        ClientSessionChannel<BusinessPacket>
        > {
    private final NetworkRouter networkRouter;
    private final AtomicReference<byte[]> businessChannelToken = new AtomicReference<>(null);

    public ClientSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey,
                         ClientChannelFactory factory, TaskScheduler scheduler,
                         NetworkRouter networkRouter) {
        super(sessionKey, signalingSecretKey, businessSecretKey, factory, scheduler);
        this.networkRouter = networkRouter;
    }

    @Override
    protected boolean connectBusinessChannel() {
        if (businessChannel.becomeConnecting()) {
            Integer port = networkRouter.getPort(AbstractPacket.PacketType.BUSINESS);
            if (port == null) {
                return pushSignalingPacket(BareSignal.BusinessChannelPort_Request, BareSignal.BusinessChannelToken_Request) == 0;
            } else {
                return pushSignalingPacket(BareSignal.BusinessChannelToken_Request);
            }
        }
        return true;
    }

    public void setBusinessChannelToken(byte[] businessChannelToken) {
        this.businessChannelToken.set(businessChannelToken);
    }

    public byte[] getBusinessChannelToken() {
        return businessChannelToken.getAndSet(null);
    }

    @Override
    public void close() {
        super.close();
        businessChannelToken.set(null);
    }

    public static class ClientChannelFactory implements ChannelFactory<
            ClientSession,
            ClientSessionChannel<SignalingPacket>,
            ClientSessionChannel<BusinessPacket>
            > {
        private final GeneralLoop loop;
        private final FragmenterFactory fragmenterFactory;
        private final AssemblerFactory assemblerFactory;

        public ClientChannelFactory(NexalithicBuilderContext context, GeneralLoop loop) {
            this.fragmenterFactory = new FragmenterFactory(context, NexalithicClient.MODULES);
            this.assemblerFactory = new AssemblerFactory(context, NexalithicClient.MODULES, NexalithicEndpoint.Type.CLIENT);
            this.loop = loop;
        }

        @Override
        public ClientSessionChannel<SignalingPacket> createSignalingChannel(ClientSession session, SecretKeyContext context) {
            return new ClientSessionChannel<>(AbstractPacket.PacketType.SIGNALING, session, loop,
                    fragmenterFactory.createSignaling(),
                    assemblerFactory.createSignaling(),
                    context);
        }

        @Override
        public ClientSessionChannel<BusinessPacket> createBusinessChannel(ClientSession session, SecretKeyContext context) {
            return new ClientSessionChannel<>(AbstractPacket.PacketType.BUSINESS, session, loop,
                    fragmenterFactory.createBusiness(session),
                    assemblerFactory.createBusiness(session),
                    context);
        }
    }
}
