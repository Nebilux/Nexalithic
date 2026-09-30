package com.nebilux.nexalithic.server.session;

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
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;

/**
 * 服务器通道工厂
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/28
 */
public class ServerChannelFactory implements ChannelFactory<ServerSession> {
    private final ServiceUnit serviceUnit;
    private final FragmenterFactory fragmenterFactory;
    private final AssemblerFactory assemblerFactory;

    public ServerChannelFactory(NexalithicBuilderContext context, ServiceUnit serviceUnit) {
        this.serviceUnit = serviceUnit;
        this.fragmenterFactory = new FragmenterFactory(context, NexalithicServer.MODULES);
        this.assemblerFactory = new AssemblerFactory(context, NexalithicServer.MODULES, NexalithicEndpoint.Type.SERVER);
    }

    @Override
    public SessionChannel<SignalingPacket, ServerSession> createSignalingChannel(ServerSession session, SecretKeyContext context) {
        return new SessionChannel<>(context, AbstractPacket.PacketType.Signaling, session, serviceUnit.getSignalingLoop(),
                fragmenterFactory.createSignaling(), assemblerFactory.createSignaling()
        );
    }

    @Override
    public SessionChannel<BusinessPacket, ServerSession> createBusinessChannel(ServerSession session, SecretKeyContext context) {
        return new SessionChannel<>(context, AbstractPacket.PacketType.Business, session, serviceUnit.selectBusinessLoop(),
                fragmenterFactory.createBusiness(session), assemblerFactory.createBusiness(session)
        );
    }
}
