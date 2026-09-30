package com.thezeroer.nexalithic.server.session;

import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.session.NexalithicSession;
import com.thezeroer.nexalithic.core.session.SessionAttachment;
import com.thezeroer.nexalithic.core.session.SessionKey;
import com.thezeroer.nexalithic.server.io.session.ServiceUnit;
import com.thezeroer.nexalithic.server.io.session.business.BusinessLoop;
import com.thezeroer.nexalithic.server.io.session.signaling.SignalingLoop;

/**
 * 服务器会话
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/09
 * @version 1.0.0
 */
public class ServerSession extends NexalithicSession<ServerSession> {
    private final ServiceUnit serviceUnit;
    private volatile SessionAttachment attachment;

    public ServerSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey,
                         ServerChannelFactory factory, TaskScheduler scheduler, ServiceUnit serviceUnit) {
        super(sessionKey, signalingSecretKey, businessSecretKey, factory, scheduler);
        this.serviceUnit = serviceUnit;
    }

    public void attach(SessionAttachment attachment) {
        this.attachment = attachment;
    }
    @SuppressWarnings("unchecked")
    public <T extends SessionAttachment> T attachment()  {
        return (T) attachment;
    }

    public SignalingLoop getSignalingLoop() {
        return (SignalingLoop) getSignalingChannel().ownerLoop();
    }
    public BusinessLoop getBusinessLoop() {
        return (BusinessLoop) getBusinessChannel().ownerLoop();
    }

    public ServiceUnit getServiceUnit() {
        return serviceUnit;
    }

    @Override
    protected boolean connectBusinessChannel() {
        if (businessChannel.beginOpen()) {
            return serviceUnit.getSignalingLoop().prepareChannelAccess(this, signalingChannel.getRemoteAddress().getAddress());
        }
        return true;
    }

    @Override
    public void onClose() {
        if (attachment != null) {
            attachment.clear();
            attachment = null;
        }
    }
}
