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
import com.nebilux.nexalithic.core.connection.channel.NexalithicChannel.Kind;
import com.nebilux.nexalithic.core.connection.channel.SessionChannel;
import com.nebilux.nexalithic.core.io.loop.ChannelLoop;
import com.nebilux.nexalithic.core.messaging.task.TaskCoordinator;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.NexalithicModel;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.ScalarSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.model.stream.file.FileStream;
import com.nebilux.nexalithic.core.model.stream.media.MediaStream;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.security.SecretKeyContexts;
import com.nebilux.nexalithic.core.util.TimeUtils;

import java.nio.channels.SelectionKey;
import java.util.List;
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
        SessionChannel<?, S> create(NexalithicChannel.Kind kind, S session, SecretKeyContext secretKeyContext);
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);
    private final SessionKey sessionKey;
    private final SessionChannel<SignalingPacket, S> signalingChannel;
    private final SessionChannel<BusinessPacket, S> businessChannel;
    private final SessionChannel<MediaStream, S> mediaChannel;
    private final SessionChannel<FileStream, S> fileChannel;
    private final List<SessionChannel<?, S>> channels;
    private final TaskCoordinator taskCoordinator;
    private final long creationTimeMillis;
    private volatile String sessionName;

    protected AbstractSession(SessionKey sessionKey, SecretKeyContexts secretKeyContexts, ChannelFactory<S> factory, TaskScheduler scheduler) {
        this.creationTimeMillis = System.currentTimeMillis();
        this.sessionKey = sessionKey;
        this.signalingChannel = (SessionChannel<SignalingPacket, S>) factory.create(Kind.Packet_Signaling, (S) this, secretKeyContexts.get(Kind.Packet_Signaling));
        this.businessChannel = (SessionChannel<BusinessPacket, S>) factory.create(Kind.Packet_Business, (S) this, secretKeyContexts.get(Kind.Packet_Business));
        this.mediaChannel = (SessionChannel<MediaStream, S>) factory.create(Kind.Stream_Media, (S) this, secretKeyContexts.get(Kind.Stream_Media));
        this.fileChannel = (SessionChannel<FileStream, S>) factory.create(Kind.Stream_File, (S) this, secretKeyContexts.get(Kind.Stream_File));
        this.channels = List.of(signalingChannel, businessChannel, mediaChannel, fileChannel);
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
        return pushModel(businessChannel, packet);
    }

    public final boolean pushFileStream(FileStream stream) {
        return pushModel(fileChannel, stream);
    }

    public final boolean pushMediaStream(MediaStream stream) {
        return pushModel(mediaChannel, stream);
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
        SessionChannel<?, S> channel = getChannel(kind);
        return channel != null && tryOpenChannel(channel);
    }

    public final SessionChannel<SignalingPacket, S> getSignalingChannel() {
        return signalingChannel;
    }
    public final SessionChannel<BusinessPacket, S> getBusinessChannel() {
        return businessChannel;
    }
    public final SessionChannel<FileStream, S> getFileChannel() {
        return fileChannel;
    }
    public final SessionChannel<MediaStream, S> getMediaChannel() {
        return mediaChannel;
    }

    /** 所有通道的待发送数据均已排空。 */
    public final boolean areChannelsWriteDrained() {
        for (SessionChannel<?, S> channel : channels) {
            if (!channel.isWriteDrained()) {
                return false;
            }
        }
        return true;
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
        long lastActive = -1;
        for (SessionChannel<?, S> channel : channels) {
            lastActive = Math.max(lastActive, channel.getLastActiveTimeNanos());
        }
        return lastActive;
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
            case Stream_Media -> mediaChannel;
            case Stream_File -> fileChannel;
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
            for (SessionChannel<?, S> channel : channels) {
                disconnectChannel(channel);
            }
        } finally {
            state.set(State.INACTIVE);
            onClose();
        }
    }

    protected abstract boolean requestChannelAccess(SessionChannel<?, S> channel);
    protected void onClose() {}

    private <M extends NexalithicModel> boolean pushModel(SessionChannel<M, S> channel, M model) {
        if (channel == null || state.get() != State.ACTIVE) {
            return false;
        }
        switch (channel.getState()) {
            case Closing -> {
                return false;
            }
            case Closed -> {
                if (!tryOpenChannel(channel)) {
                    return false;
                }
            }
            case Opening, Opened -> {}
        }
        if (!channel.put(model)) {
            return false;
        }
        if (channel.getState() == NexalithicChannel.State.Opened && channel.hasReadyOutput()) {
            channel.updateInterest(SelectionKey.OP_WRITE, true);
        }
        return true;
    }

    private <M extends NexalithicModel> void disconnectChannel(SessionChannel<M, S> channel) {
        if (channel.isOpen()) {
            channel.getOwnerLoop().submitDisconnectEvent(
                    channel, ChannelLoop.Event.Disconnect.Reason.LOCAL_REQUEST
            );
        }
    }

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
        return "SessionName: " + sessionName + ", CreationTime: " + TimeUtils.format(creationTimeMillis) + ", Channels" + channels;
    }
}
