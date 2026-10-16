/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.server.session;

import com.nebilux.nexalithic.core.connection.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.connection.channel.NexalithicChannel.Kind;
import com.nebilux.nexalithic.core.connection.channel.SessionBusinessChannel;
import com.nebilux.nexalithic.core.connection.channel.SessionChannel;
import com.nebilux.nexalithic.core.connection.channel.SessionSignalingChannel;
import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.connection.session.SessionAttachment;
import com.nebilux.nexalithic.core.connection.session.SessionKey;
import com.nebilux.nexalithic.core.io.codec.AssemblerFactory;
import com.nebilux.nexalithic.core.io.codec.CodecResources;
import com.nebilux.nexalithic.core.io.codec.FragmenterFactory;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContexts;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.io.session.business.BusinessLoop;
import com.nebilux.nexalithic.server.io.session.signaling.SignalingLoop;
import com.nebilux.nexalithic.server.session.access.ChannelAccessCoordinator;

import java.util.EnumMap;

/**
 * 服务器会话
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ServerSession extends AbstractSession<ServerSession> {
    private final ServiceUnit serviceUnit;
    private final ChannelAccessCoordinator channelAccessCoordinator;
    private volatile SessionAttachment attachment;

    public ServerSession(SessionKey sessionKey, SecretKeyContexts secretKeyContexts,
                         ChannelFactory factory, TaskScheduler scheduler, ServiceUnit serviceUnit,
                         ChannelAccessCoordinator channelAccessCoordinator) {
        super(sessionKey, secretKeyContexts, factory, scheduler);
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
        return (BusinessLoop) this.<BusinessPacket>getBusinessChannel(Kind.Business_Packet).getOwnerLoop();
    }

    public ServiceUnit getServiceUnit() {
        return serviceUnit;
    }

    @Override
    protected boolean requestChannelAccess(SessionChannel<?, ServerSession> channel) {
        if (channel.getKind() != NexalithicChannel.Kind.Business_Packet) {
            return false;
        }
        return channelAccessCoordinator.issue(
                this, channel.getKind(), getSignalingChannel().getRemoteAddress().getAddress()
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
    public static class ChannelFactory implements AbstractSession.ChannelFactory<ServerSession> {
        private final ServiceUnit serviceUnit;
        private final FragmenterFactory fragmenterFactory;
        private final AssemblerFactory assemblerFactory;

        public ChannelFactory(ServiceUnit serviceUnit, CodecResources resources) {
            this.serviceUnit = serviceUnit;
            this.fragmenterFactory = resources.fragmenters();
            this.assemblerFactory = resources.assemblers();
        }

        @Override
        public ChannelGroup<ServerSession> create(ServerSession session, SecretKeyContexts secretKeyContexts) {
            SessionSignalingChannel<ServerSession> signaling = new SessionSignalingChannel<>(
                    serviceUnit.getSignalingLoop(), session, fragmenterFactory.createSignaling(),
                    assemblerFactory.createSignaling(), secretKeyContexts.get(Kind.Signaling_Packet));
            EnumMap<Kind, SessionBusinessChannel<?, ServerSession>> business = new EnumMap<>(Kind.class);
            business.put(Kind.Business_Packet, new SessionBusinessChannel<>(Kind.Business_Packet,
                    serviceUnit.selectBusinessLoop(), session, fragmenterFactory.createBusiness(session),
                    assemblerFactory.createBusiness(session), secretKeyContexts.get(Kind.Business_Packet)));
            business.put(Kind.Business_Stream_Media, new SessionBusinessChannel<>(Kind.Business_Stream_Media,
                    serviceUnit.selectBusinessLoop(), session, fragmenterFactory.createMedia(), assemblerFactory.createMedia(),
                    secretKeyContexts.get(Kind.Business_Stream_Media)));
            business.put(Kind.Business_Stream_File, new SessionBusinessChannel<>(Kind.Business_Stream_File,
                    serviceUnit.selectBusinessLoop(), session, fragmenterFactory.createFile(), assemblerFactory.createFile(),
                    secretKeyContexts.get(Kind.Business_Stream_File)));
            return new ChannelGroup<>(signaling, business);
        }
    }
}
