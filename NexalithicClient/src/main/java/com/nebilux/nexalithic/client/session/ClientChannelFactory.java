package com.nebilux.nexalithic.client.session;

import com.nebilux.nexalithic.client.NexalithicClient;
import com.nebilux.nexalithic.client.io.session.ClientSessionLoop;
import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.io.codec.AssemblerFactory;
import com.nebilux.nexalithic.core.io.codec.FragmenterFactory;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.session.ChannelFactory;
import com.nebilux.nexalithic.core.session.SessionChannel;

/**
 * 客户端通道工厂
 *
 * @author Reonvia
 * @since 0.1.0
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
