package com.nebilux.nexalithic.core.session;

import com.nebilux.nexalithic.core.io.loop.ChannelLoop;
import com.nebilux.nexalithic.core.messaging.task.TaskCoordinator;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.util.TimeUtils;

import java.nio.channels.SelectionKey;

/**
 * Nexalithic 会话
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/02
 * @version 1.0.0
 */
@SuppressWarnings("unchecked")
public abstract class NexalithicSession <S extends NexalithicSession<S>> {
    protected final SessionKey sessionKey;
    protected final SessionChannel<SignalingPacket, S> signalingChannel;
    protected final SessionChannel<BusinessPacket, S> businessChannel;
    protected final TaskCoordinator taskCoordinator;
    protected final long creationTimeMillis;
    protected volatile String sessionName;

    public NexalithicSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey,
                             ChannelFactory<S> factory, TaskScheduler scheduler) {
        this.creationTimeMillis = System.currentTimeMillis();
        this.sessionKey = sessionKey;
        this.signalingChannel = factory.createSignalingChannel((S) this, signalingSecretKey);
        this.businessChannel = factory.createBusinessChannel((S) this, businessSecretKey);
        this.taskCoordinator = new TaskCoordinator(this, scheduler);
    }

    public final boolean pushSignalingPacket(SignalingPacket packet) {
        if (!signalingChannel.put(packet)) {
            return false;
        }
        signalingChannel.updateInterest(SelectionKey.OP_WRITE, true);
        return true;
    }
    public final int pushSignalingPacket(SignalingPacket... packets) {
        int count = signalingChannel.fill(packets);
        if (count != packets.length) {
            signalingChannel.updateInterest(SelectionKey.OP_WRITE, true);
        }
        return count;
    }
    public final boolean pushBusinessPacket(BusinessPacket packet) {
        if (!businessChannel.put(packet)) {
            return false;
        }
        switch (businessChannel.getState()) {
            case Closed -> {
                return connectBusinessChannel();
            }
            case Opened -> {
                businessChannel.updateInterest(SelectionKey.OP_WRITE, true);
            }
        }
        return true;
    }

    public final SessionChannel<SignalingPacket, S> getSignalingChannel() {
        return signalingChannel;
    }
    public final SessionChannel<BusinessPacket, S> getBusinessChannel() {
        return businessChannel;
    }
    public final SessionChannel<?, S> getChannel(AbstractPacket.PacketType packetType) {
        return switch (packetType) {
            case Signaling -> signalingChannel;
            case Business -> businessChannel;
        };
    }
    public final <C extends SessionChannel<?, S>> C asChannel(AbstractPacket.PacketType packetType) {
        return (C) switch (packetType) {
            case Signaling -> signalingChannel;
            case Business -> businessChannel;
        };
    }

    public final void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }
    public final String getSessionName() {
        return sessionName;
    }

    public final SessionKey getSessionKey() {
        return sessionKey;
    }
    public final TaskCoordinator getTaskCoordinator() {
        return taskCoordinator;
    }
    public final long getCreationTimeMillis() {
        return creationTimeMillis;
    }
    public final long getLastActiveTimeNanos() {
        return Math.max(signalingChannel.getLastActiveTimeNanos(), businessChannel.getLastActiveTimeNanos());
    }

    public final void setRemoteBusinessChannelWriteRate(long rate) {
        businessChannel.updateReadRate((long) (rate * 1.2));
        pushSignalingPacket(ScalarSignal.ofLong(SignalingPacket.Signal.BusinessChannelRate, rate));
    }

    public final void close() {
        if (signalingChannel.isOpen()) {
            signalingChannel.ownerLoop().submitDisconnectEvent(signalingChannel, ChannelLoop.Event.Disconnect.Reason.LOCAL_REQUEST);
        }
        if (businessChannel.isOpen()) {
            businessChannel.ownerLoop().submitDisconnectEvent(businessChannel, ChannelLoop.Event.Disconnect.Reason.LOCAL_REQUEST);
        }
        onClose();
    }

    protected abstract boolean connectBusinessChannel();
    protected void onClose() {}

    @Override
    public String toString() {
        return "SessionName: " + sessionName + ", CreationTime: " + TimeUtils.format(creationTimeMillis) + ", SignalingChannel[" + signalingChannel + "], BusinessChannel[" + businessChannel + "]";
    }
}
