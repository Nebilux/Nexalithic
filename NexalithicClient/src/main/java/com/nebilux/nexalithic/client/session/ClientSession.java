package com.nebilux.nexalithic.client.session;

import com.nebilux.nexalithic.client.manager.NetworkRouter;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.BareSignal;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import com.nebilux.nexalithic.core.session.SessionKey;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 客户端会话
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/09
 */
public class ClientSession extends NexalithicSession<ClientSession> {
    private final NetworkRouter networkRouter;
    private final AtomicReference<byte[]> businessChannelToken = new AtomicReference<>(null);

    public ClientSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey,
                         ClientChannelFactory factory, TaskScheduler scheduler, NetworkRouter networkRouter) {
        super(sessionKey, signalingSecretKey, businessSecretKey, factory, scheduler);
        this.networkRouter = networkRouter;
    }

    @Override
    protected boolean connectBusinessChannel() {
        if (businessChannel.beginOpen()) {
            Integer port = networkRouter.getPort(AbstractPacket.PacketType.Business);
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
    public void onClose() {
        businessChannelToken.set(null);
    }
}
