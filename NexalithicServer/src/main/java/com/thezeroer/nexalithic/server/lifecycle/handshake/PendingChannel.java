package com.thezeroer.nexalithic.server.lifecycle.handshake;

import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.SelfStaticRecyclableWrapper;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.session.SessionKey;
import com.thezeroer.nexalithic.core.session.channel.NexalithicChannel;
import com.thezeroer.nexalithic.server.lifecycle.service.session.ServerSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.PrivateKey;

/**
 * 待定通道
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/07
 * @version 1.0.0
 */
public class PendingChannel extends SelfStaticRecyclableWrapper<PendingChannel> implements NexalithicChannel {
    public record Constant(long MaxWaitTimeNanos, int readBufferCapacity, int writeBufferCapacity) {}
    public enum State {
        STEP_1,
        STEP_2,
    }

    private final Constant CONSTANT;
    private final ByteBuffer readBuffer;
    private final ByteBuffer writeBuffer;
    private volatile AbstractPacket.PacketType type;
    private volatile SelectionKey selectionKey;
    private volatile SocketChannel socketChannel;
    private volatile State state;
    private volatile PrivateKey privateKey;
    private volatile MessageDigest transcriptHash;
    private volatile ServerSession session;
    private volatile SessionKey sessionKey;
    private volatile SecretKeyContext signalingSecretContext, businessSecretContext;
    private volatile long lastActiveTimeNanos = -1;

    public PendingChannel(GenericWrapperPool<PendingChannel, PendingChannel> owner, Constant constant) {
        super(owner);
        CONSTANT = constant;
        readBuffer = ByteBuffer.allocate(constant.readBufferCapacity);
        writeBuffer = ByteBuffer.allocate(constant.writeBufferCapacity);
    }

    public PendingChannel init(AbstractPacket.PacketType packetType, SocketChannel socketChannel) {
        this.type = packetType;
        this.socketChannel = socketChannel;
        state = State.STEP_1;
        lastActiveTimeNanos = System.nanoTime();
        return this;
    }

    public AbstractPacket.PacketType getType() {
        return type;
    }
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void setState(State state) {
        this.state = state;
    }
    public State getState() {
        return state;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }
    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }

    public PendingChannel setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
        return this;
    }
    public SelectionKey getSelectionKey() {
        return selectionKey;
    }
    public PendingChannel setPrivateKey(PrivateKey privateKey) {
        this.privateKey = privateKey;
        return this;
    }
    public PrivateKey getPrivateKey() {
        return privateKey;
    }
    public PendingChannel setTranscriptHash(MessageDigest transcriptHash) {
        this.transcriptHash = transcriptHash;
        return this;
    }
    public MessageDigest getTranscriptHash() {
        return transcriptHash;
    }
    public PendingChannel setSession(ServerSession session) {
        this.session = session;
        return this;
    }
    public ServerSession getSession() {
        return session;
    }
    public PendingChannel setSessionKey(SessionKey sessionKey) {
        this.sessionKey = sessionKey;
        return this;
    }
    public SessionKey getSessionKey() {
        return sessionKey;
    }
    public PendingChannel setSignalingSecretContext(SecretKeyContext signalingSecretContext) {
        this.signalingSecretContext = signalingSecretContext;
        return this;
    }
    public SecretKeyContext getSignalingSecretContext() {
        return signalingSecretContext;
    }
    public PendingChannel setBusinessSecretContext(SecretKeyContext businessSecretContext) {
        this.businessSecretContext = businessSecretContext;
        return this;
    }
    public SecretKeyContext getBusinessSecretContext() {
        return businessSecretContext;
    }

    public long getExpiryTimeNanos() {
        return lastActiveTimeNanos + CONSTANT.MaxWaitTimeNanos;
    }

    @Override
    public void updateLastActiveTimeNanos(long lastActiveTimeNanos) {
        this.lastActiveTimeNanos = lastActiveTimeNanos;
    }
    @Override
    public long getLastActiveTimeNanos() {
        return lastActiveTimeNanos;
    }

    @Override
    public boolean closeChannel() {
        if (!isActive()) {
            return false;
        }
        try {
            if (selectionKey != null) {
                selectionKey.cancel();
                selectionKey = null;
            }
            if (socketChannel != null) {
                socketChannel.close();
                socketChannel = null;
            }
        } catch (IOException ignored) {}
        recycle();
        return true;
    }

    @Override
    protected void onReset() {
        readBuffer.clear();
        writeBuffer.clear();
        type = null;
        socketChannel = null;
        selectionKey = null;
        state = null;
        privateKey = null;
        transcriptHash = null;
        session = null;
        sessionKey = null;
        signalingSecretContext = null;
        businessSecretContext = null;
        lastActiveTimeNanos = -1;
    }

    @Override
    public String toString() {
        return "PacketType: " + type + ", State: " + state + ", SocketChannel: " + socketChannel;
    }
}
