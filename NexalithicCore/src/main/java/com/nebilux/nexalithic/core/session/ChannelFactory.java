package com.nebilux.nexalithic.core.session;

import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;

/**
 * 通道工厂
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface ChannelFactory<S extends NexalithicSession<S>> {
    SessionChannel<SignalingPacket, S> createSignalingChannel(S session, SecretKeyContext secretKeyContext);
    SessionChannel<BusinessPacket, S> createBusinessChannel(S session, SecretKeyContext secretKeyContext);
}
