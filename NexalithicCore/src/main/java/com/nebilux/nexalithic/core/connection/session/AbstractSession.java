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

package com.nebilux.nexalithic.core.connection.session;

import com.nebilux.nexalithic.core.connection.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.connection.channel.SessionChannel;
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
 * 抽象会话
 *
 * @author Reonvia
 * @since 0.1.0
 */
@SuppressWarnings("unchecked")
public abstract class AbstractSession<S extends AbstractSession<S>> implements NexalithicSession {
    /**
     * 会话通道工厂
     *
     * @author Reonvia
     * @since 0.1.0
     */
    public interface ChannelFactory<S extends AbstractSession<S>> {
        SessionChannel<SignalingPacket, S> createSignalingChannel(S session, SecretKeyContext secretKeyContext);
        SessionChannel<BusinessPacket, S> createBusinessChannel(S session, SecretKeyContext secretKeyContext);
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);
    protected final SessionKey sessionKey;
    protected final SessionChannel<SignalingPacket, S> signalingChannel;
    protected final SessionChannel<BusinessPacket, S> businessChannel;
    protected final TaskCoordinator taskCoordinator;
    protected final long creationTimeMillis;
    protected volatile String sessionName;

    public AbstractSession(SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey, ChannelFactory<S> factory, TaskScheduler scheduler) {
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
        switch (businessChannel.getState()) {
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

    public final void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }
    public final void setRemoteBusinessChannelWriteRate(long rate) {
        businessChannel.updateReadRate((long) (rate * 1.2));
        pushSignalingPacket(ScalarSignal.ofLong(SignalingPacket.Signal.BusinessChannelRate, rate));
    }

    public final String getSessionName() {
        return sessionName;
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

    @Override
    public final SessionKey getSessionKey() {
        return sessionKey;
    }

    @Override
    public final SessionChannel<?, S> getChannel(NexalithicChannel.Kind kind) {
        return switch (kind) {
            case Packet_Signaling -> signalingChannel;
            case Packet_Business -> businessChannel;
            default -> throw new IllegalArgumentException("Unknown channel kind: " + kind);
        };
    }

    @Override
    public final State getState() {
        return state.get();
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
    @Override
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
