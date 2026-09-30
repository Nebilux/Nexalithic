package com.nebilux.nexalithic.core.session;

import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;

/**
 * 通道工厂
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/28
 */
public interface ChannelFactory<S extends NexalithicSession<S>> {
    SessionChannel<SignalingPacket, S> createSignalingChannel(S session, SecretKeyContext secretKeyContext);
    SessionChannel<BusinessPacket, S> createBusinessChannel(S session, SecretKeyContext secretKeyContext);
}
