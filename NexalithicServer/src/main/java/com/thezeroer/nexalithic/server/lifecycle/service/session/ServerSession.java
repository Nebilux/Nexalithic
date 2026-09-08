package com.thezeroer.nexalithic.server.lifecycle.service.session;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.io.codec.AssemblerFactory;
import com.thezeroer.nexalithic.core.io.codec.FragmenterFactory;
import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.session.NexalithicSession;
import com.thezeroer.nexalithic.core.session.SessionAttachment;
import com.thezeroer.nexalithic.core.session.SessionKey;
import com.thezeroer.nexalithic.core.session.channel.ChannelFactory;
import com.thezeroer.nexalithic.server.lifecycle.service.ServiceUnit;
import com.thezeroer.nexalithic.server.lifecycle.service.StewardLoop;
import com.thezeroer.nexalithic.server.lifecycle.service.WorkerLoop;

import java.util.concurrent.TimeUnit;

/**
 * 服务器会话
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/09
 * @version 1.0.0
 */
public class ServerSession extends NexalithicSession<
        ServerSession,
        ServerSessionChannel<SignalingPacket>,
        ServerSessionChannel<BusinessPacket>
    > {
    public record Constant(long HeartBeat_MaxIntervalNanos) {}
    private final Constant CONSTANT;
    private final ServiceUnit serviceUnit;
    private volatile SessionAttachment attachment;

    public ServerSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey,
                         ServerChannelFactory factory, TaskScheduler scheduler, Constant constant, ServiceUnit serviceUnit) {
        super(sessionKey, signalingSecretKey, businessSecretKey, factory, scheduler);
        this.CONSTANT = constant;
        this.serviceUnit = serviceUnit;
    }

    @Override
    protected boolean connectBusinessChannel() {
        if (businessChannel.becomeConnecting()) {
            return getSignalingChannel().<StewardLoop>asLocalLoop().prepareChannelAccess(this, AbstractPacket.PacketType.BUSINESS, signalingChannel.getRemoteAddress().getAddress());
        }
        return true;
    }

    public ServiceUnit getServiceUnit() {
        return serviceUnit;
    }

    public long getExpiryTimeNanos() {
        return lastActiveTimeNanos + CONSTANT.HeartBeat_MaxIntervalNanos;
    }

    public void attach(SessionAttachment attachment) {
        this.attachment = attachment;
    }
    @SuppressWarnings("unchecked")
    public <T extends SessionAttachment> T attachment()  {
        return (T) attachment;
    }

    @Override
    public void close() {
        super.close();
        if (attachment != null) {
            attachment.clear();
            attachment = null;
        }
    }

    public static class ServerChannelFactory implements ChannelFactory <
            ServerSession,
            ServerSessionChannel<SignalingPacket>,
            ServerSessionChannel<BusinessPacket>
            > {

        private final StewardLoop loop;
        private final FragmenterFactory fragmenterFactory;
        private final AssemblerFactory assemblerFactory;
        private final ServerSessionChannel.Constant serverSessionChannelConstant;

        public ServerChannelFactory(NexalithicBuilderContext context, StewardLoop loop) {
            this.fragmenterFactory = new FragmenterFactory(context);
            this.assemblerFactory = new AssemblerFactory(context);
            this.loop = loop;
            serverSessionChannelConstant = context.getConstant(ServerSessionChannel.class, ServerSessionChannel.Constant.class, () -> new ServerSessionChannel.Constant(
                    TimeUnit.NANOSECONDS.convert(context.getOption(WorkerLoop.OPTIONS.MaxIdleTimeMillis), TimeUnit.MILLISECONDS))
            );
        }

        @Override
        public ServerSessionChannel<SignalingPacket> createSignalingChannel(ServerSession session, SecretKeyContext context) {
            return new ServerSessionChannel<>(AbstractPacket.PacketType.SIGNALING, session, loop,
                    fragmenterFactory.createSignaling(),
                    assemblerFactory.createSignaling(),
                    context, serverSessionChannelConstant);
        }

        @Override
        public ServerSessionChannel<BusinessPacket> createBusinessChannel(ServerSession session, SecretKeyContext context) {
            return new ServerSessionChannel<>(AbstractPacket.PacketType.BUSINESS, session, null,
                    fragmenterFactory.createBusiness(session),
                    assemblerFactory.createBusiness(session),
                    context, serverSessionChannelConstant);
        }
    }
}
