package com.thezeroer.nexalithic.core.session;

import com.thezeroer.nexalithic.core.io.loop.ChannelLoop;
import com.thezeroer.nexalithic.core.messaging.task.TaskCoordinator;
import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.thezeroer.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.session.channel.ChannelFactory;
import com.thezeroer.nexalithic.core.session.channel.SessionChannel;
import com.thezeroer.nexalithic.core.util.TimeUtils;

import java.nio.channels.SelectionKey;
import java.util.concurrent.locks.LockSupport;

/**
 * Nexalithic 会话
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/02
 * @version 1.0.0
 */
@SuppressWarnings("unchecked")
public abstract class NexalithicSession <
        S extends NexalithicSession<S, SC, BC>,
        SC extends SessionChannel<SignalingPacket, S>,
        BC extends SessionChannel<BusinessPacket, S>
    > {
    public static final int SESSION_KEY_LENGTH = SessionKey.LENGTH;
    protected final long creationTime;
    protected final SessionKey sessionKey;
    protected final SC signalingChannel;
    protected final BC businessChannel;
    protected final TaskCoordinator taskCoordinator;
    protected volatile String sessionName;
    protected volatile long lastActiveTimeNanos = -1;

    public NexalithicSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey,
                             ChannelFactory<S, SC, BC> factory, TaskScheduler scheduler) {
        this.creationTime = System.currentTimeMillis();
        this.sessionKey = sessionKey;
        this.signalingChannel = factory.createSignalingChannel((S) this, signalingSecretKey);
        this.businessChannel = factory.createBusinessChannel((S) this, businessSecretKey);
        this.taskCoordinator = new TaskCoordinator(this, scheduler);
    }

    public final boolean pushSignalingPacket(SignalingPacket packet) {
        if (!signalingChannel.put(packet)) {
            return false;
        }
        if (signalingChannel.updateChannelInterest(SelectionKey.OP_WRITE, true)) {
            return updateChannelInterest(signalingChannel);
        }
        return true;
    }
    public final int pushSignalingPacket(SignalingPacket... packets) {
        int count = signalingChannel.fill(packets);
        if (count != packets.length) {
            if (signalingChannel.updateChannelInterest(SelectionKey.OP_WRITE, true)) {
                if (!updateChannelInterest(signalingChannel)) {
                    return -1;
                }
            }
        }
        return count;
    }
    public final boolean pushBusinessPacket(BusinessPacket packet) {
        if (!businessChannel.put(packet)) {
            return false;
        }
        switch (businessChannel.getState()) {
            case Unconnected -> {
                return connectBusinessChannel();
            }
            case Connected -> {
                if (businessChannel.updateChannelInterest(SelectionKey.OP_WRITE, true)) {
                    return updateChannelInterest(businessChannel);
                }
            }
        }
        return true;
    }
    public final int pushBusinessPacket(BusinessPacket... packets) {
        int count = businessChannel.fill(packets);
        switch (businessChannel.getState()) {
            case Unconnected -> {
                if (!connectBusinessChannel()) {
                    return -1;
                }
            }
            case Connected -> {
                if (count != packets.length) {
                    if (businessChannel.updateChannelInterest(SelectionKey.OP_WRITE, true)) {
                        if (!updateChannelInterest(businessChannel)) {
                            return -1;
                        }
                    }
                }
            }
        }
        return count;
    }

    public final SC getSignalingChannel() {
        return signalingChannel;
    }
    public final BC getBusinessChannel() {
        return businessChannel;
    }
    public final SessionChannel<?, S> getChannel(AbstractPacket.PacketType packetType) {
        return switch (packetType) {
            case SIGNALING -> signalingChannel;
            case BUSINESS -> businessChannel;
        };
    }
    public final <C extends SessionChannel<?, S>> C asChannel(AbstractPacket.PacketType packetType) {
        return (C) switch (packetType) {
            case SIGNALING -> signalingChannel;
            case BUSINESS -> businessChannel;
        };
    }

    public final void updateLastActiveTimeNanos(long lastActiveTimeNanos) {
        this.lastActiveTimeNanos = lastActiveTimeNanos;
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
    public final long getCreationTime() {
        return creationTime;
    }
    public final long getLastActiveTimeNanos() {
        return lastActiveTimeNanos;
    }

    public final void setRemoteBusinessChannelWriteRate(long rate) {
        businessChannel.updateReadRate((long) (rate * 1.2));
        pushSignalingPacket(ScalarSignal.ofLong(SignalingPacket.Signal.BusinessChannelRate, rate));
    }

    public void close() {
        lastActiveTimeNanos = -1;
        if (signalingChannel != null) {
            signalingChannel.closeChannel();
        }
        if (businessChannel != null) {
            businessChannel.closeChannel();
        }
    }

    protected abstract boolean connectBusinessChannel();

    private boolean updateChannelInterest(SessionChannel<?, ?> channel) {
        ChannelLoop<?> loop = channel.localLoop();
        if (loop != null) {
            loop.updateChannelInterest(channel);
            return true;
        } else {
            for (int i = 0; i < 100; i++) {
                if (loop != null) {
                    loop.updateChannelInterest(channel);
                    return true;
                } else {
                    if (i < 50) {
                        Thread.onSpinWait();
                    } else {
                        LockSupport.parkNanos(i * 1_000_000L);
                    }
                }
                loop = channel.localLoop();
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "SessionName: " + sessionName + ", CreationTime: " + TimeUtils.format(creationTime) + ", SignalingChannel[" + signalingChannel + "], BusinessChannel[" + businessChannel + "]";
    }
}
