package com.nebilux.nexalithic.core.session;

import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.loop.ChannelLoop;
import com.nebilux.nexalithic.core.messaging.task.TaskCoordinator;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.util.TimeUtils;

import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Nexalithic 会话
 *
 * @author Reonvia
 * @since 0.1.0
 */
@SuppressWarnings("unchecked")
public abstract class NexalithicSession <S extends NexalithicSession<S>> {
    /** Session 的逻辑生命周期。 */
    public enum State {
        /** Session 逻辑身份有效，可以处理数据和建立附属 Channel。 */
        ACTIVE,

        /** 已经领取停用权，正在断开底层 Channel 并执行收尾。 */
        DEACTIVATING,

        /** Session 逻辑身份已经失效，不再允许任何操作。 */
        INACTIVE
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);
    protected final SessionKey sessionKey;
    protected final SessionChannel<SignalingPacket, S> signalingChannel;
    protected final SessionChannel<BusinessPacket, S> businessChannel;
    protected final TaskCoordinator taskCoordinator;
    protected final long creationTimeMillis;
    protected volatile String sessionName;

    public NexalithicSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey, SessionChannelFactory<S> factory, TaskScheduler scheduler) {
        this.creationTimeMillis = System.currentTimeMillis();
        this.sessionKey = sessionKey;
        this.signalingChannel = factory.createSignalingChannel((S) this, signalingSecretKey);
        this.businessChannel = factory.createBusinessChannel((S) this, businessSecretKey);
        this.taskCoordinator = new TaskCoordinator(this, scheduler);
    }

    public final boolean pushSignalingPacket(SignalingPacket packet) {
        if (state.get() != State.ACTIVE) {
            return false;
        }
        if (!signalingChannel.put(packet)) {
            return false;
        }
        signalingChannel.updateInterest(SelectionKey.OP_WRITE, true);
        return true;
    }

    public final boolean pushBusinessPacket(BusinessPacket packet) {
        if (state.get() != State.ACTIVE) {
            return false;
        }
        NexalithicChannel.State channelState = businessChannel.getState();
        switch (channelState) {
            case Closing -> {
                return false;
            }
            case Closed -> {
                if (!tryOpenChannel(businessChannel)) {
                    return false;
                }
            }
            case Opening, Opened -> {}
        }
        if (!businessChannel.put(packet)) {
            return false;
        }
        if (businessChannel.getState() == NexalithicChannel.State.Opened) {
            businessChannel.updateInterest(SelectionKey.OP_WRITE, true);
        }
        return true;
    }

    /**
     * 尝试异步打开指定类型的附属 Channel。
     *
     * @return 已经打开、正在打开或成功发起打开请求时返回 true
     */
    public final boolean tryOpenChannel(NexalithicChannel.Kind kind) {
        if (state.get() != State.ACTIVE) {
            return false;
        }
        return tryOpenChannel(getChannel(kind));
    }

    public final SessionChannel<SignalingPacket, S> getSignalingChannel() {
        return signalingChannel;
    }
    public final SessionChannel<BusinessPacket, S> getBusinessChannel() {
        return businessChannel;
    }
    public final SessionChannel<?, S> getChannel(NexalithicChannel.Kind kind) {
        return switch (kind) {
            case Packet_Signaling -> signalingChannel;
            case Packet_Business -> businessChannel;
            default -> throw new IllegalArgumentException("Unknown channel kind: " + kind);
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
    public final State getState() {
        return state.get();
    }
    public final long getLastActiveTimeNanos() {
        return Math.max(signalingChannel.getLastActiveTimeNanos(), businessChannel.getLastActiveTimeNanos());
    }

    public final void setRemoteBusinessChannelWriteRate(long rate) {
        businessChannel.updateReadRate((long) (rate * 1.2));
        pushSignalingPacket(ScalarSignal.ofLong(SignalingPacket.Signal.BusinessChannelRate, rate));
    }

    /**
     * 关闭当前 Session。
     *
     * <p>只有第一个成功将状态从 {@link State#ACTIVE} 转换到
     * {@link State#DEACTIVATING} 的调用者负责提交通道断开请求并执行收尾。重复调用不会重复
     * 提交断开事件，也不会重复调用 {@link #onClose()}。</p>
     *
     * <p>Session 的关闭表示逻辑身份立即失效；底层 Channel 的物理关闭仍由所属
     * {@link ChannelLoop} 串行执行。</p>
     */
    public final void close() {
        if (!state.compareAndSet(State.ACTIVE, State.DEACTIVATING)) {
            return;
        }
        try {
            if (signalingChannel.isOpen()) {
                signalingChannel.getOwnerLoop().submitDisconnectEvent(
                        signalingChannel,
                        ChannelLoop.Event.Disconnect.Reason.LOCAL_REQUEST
                );
            }
            if (businessChannel.isOpen()) {
                businessChannel.getOwnerLoop().submitDisconnectEvent(
                        businessChannel,
                        ChannelLoop.Event.Disconnect.Reason.LOCAL_REQUEST
                );
            }
        } finally {
            state.set(State.INACTIVE);
            onClose();
        }
    }

    protected abstract boolean requestChannelAccess(SessionChannel<?, S> channel);
    protected void onClose() {}

    private boolean tryOpenChannel(SessionChannel<?, S> channel) {
        if (!channel.beginOpen()) {
            NexalithicChannel.State channelState = channel.getState();
            return channelState == NexalithicChannel.State.Opening || channelState == NexalithicChannel.State.Opened;
        }
        boolean requested = requestChannelAccess(channel);
        if (!requested) {
            channel.abortOpen();
        }
        return requested;
    }

    @Override
    public String toString() {
        return "SessionName: " + sessionName + ", CreationTime: " + TimeUtils.format(creationTimeMillis) + ", SignalingChannel[" + signalingChannel + "], BusinessChannel[" + businessChannel + "]";
    }
}
