package com.thezeroer.nexalithic.client.session;

import com.thezeroer.nexalithic.client.NexalithicClient;
import com.thezeroer.nexalithic.client.io.session.ClientSessionLoop;
import com.thezeroer.nexalithic.core.NexalithicEndpoint;
import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.io.codec.AssemblerFactory;
import com.thezeroer.nexalithic.core.io.codec.FragmenterFactory;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.session.ChannelFactory;
import com.thezeroer.nexalithic.core.session.SessionChannel;

/**
 * 客户端通道工厂
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/28
 */
public class ClientChannelFactory implements ChannelFactory<ClientSession> {
    private final ClientSessionLoop loop;
    private final FragmenterFactory fragmenterFactory;
    private final AssemblerFactory assemblerFactory;

    public ClientChannelFactory(NexalithicBuilderContext context, ClientSessionLoop loop) {
        this.fragmenterFactory = new FragmenterFactory(context, NexalithicClient.MODULES);
        this.assemblerFactory = new AssemblerFactory(context, NexalithicClient.MODULES, NexalithicEndpoint.Type.CLIENT);
        this.loop = loop;
    }

    @Override
    public SessionChannel<SignalingPacket, ClientSession> createSignalingChannel(ClientSession session, SecretKeyContext context) {
        return new SessionChannel<>(context, AbstractPacket.PacketType.Signaling, session, loop,
                fragmenterFactory.createSignaling(), assemblerFactory.createSignaling()
        );
    }

    @Override
    public SessionChannel<BusinessPacket, ClientSession> createBusinessChannel(ClientSession session, SecretKeyContext context) {
        return new SessionChannel<>(context, AbstractPacket.PacketType.Business, session, loop,
                fragmenterFactory.createBusiness(session), assemblerFactory.createBusiness(session)
        );
    }
}
