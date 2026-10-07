package com.nebilux.nexalithic.client.io.session;

import com.nebilux.nexalithic.client.session.ClientSession;
import com.nebilux.nexalithic.core.io.loop.ChannelLoop;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.session.SessionKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 客户端已经完成连接前置操作、等待 {@code ClientSessionLoop} 接管的通道资源。
 *
 * <p>交接对象在创建时拥有 {@link SocketChannel}。成功调用 {@link #takeChannel()} 后，
 * Socket 所有权转移给接收方；否则 {@link #discard()} 会关闭仍由交接对象持有的 Socket。</p>
 *
 * @author Reonvia
 * @since 0.3.0
 */
public abstract sealed class ClientChannelHandoff implements ChannelLoop.Handoff permits ClientChannelHandoff.Signaling, ClientChannelHandoff.Business {
    private final long stamp;
    private final AtomicReference<SocketChannel> channel;

    protected ClientChannelHandoff(long stamp, SocketChannel channel) {
        this.stamp = stamp;
        this.channel = new AtomicReference<>(Objects.requireNonNull(channel, "channel"));
    }

    /**
     * 返回创建本次交接时的连接 stamp。
     *
     * <p>主动断开或开始新的信令连接会推进 stamp，使旧的异步连接结果无法重新接入。</p>
     */
    public final long stamp() {
        return stamp;
    }

    /**
     * 一次性移出物理连接。
     *
     * @return 当前交接对象持有的 SocketChannel
     * @throws IllegalStateException 物理连接已经被移出或丢弃
     */
    public final SocketChannel takeChannel() {
        SocketChannel result = channel.getAndSet(null);
        if (result == null) {
            throw new IllegalStateException("SocketChannel has already been transferred or discarded");
        }
        return result;
    }

    /**
     * 丢弃尚未被接管的物理连接。
     */
    @Override
    public final void discard() {
        SocketChannel remaining = channel.getAndSet(null);
        if (remaining == null) {
            return;
        }
        try {
            remaining.close();
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to discard client channel handoff", exception);
        }
    }

    /** 信令通道握手结果。 */
    public static final class Signaling extends ClientChannelHandoff {
        private final SessionKey sessionKey;
        private final SecretKeyContext signalingSecretKey;
        private final SecretKeyContext businessSecretKey;
        private final int reconnectAttempt;

        public Signaling(long stamp, SocketChannel channel, SessionKey sessionKey, SecretKeyContext signalingSecretKey, SecretKeyContext businessSecretKey, int reconnectAttempt) {
            super(stamp, channel);
            this.sessionKey = Objects.requireNonNull(sessionKey, "sessionKey");
            this.signalingSecretKey = Objects.requireNonNull(signalingSecretKey, "signalingSecretKey");
            this.businessSecretKey = Objects.requireNonNull(businessSecretKey, "businessSecretKey");
            this.reconnectAttempt = reconnectAttempt;
        }

        public SessionKey sessionKey() {
            return sessionKey;
        }

        public SecretKeyContext signalingSecretKey() {
            return signalingSecretKey;
        }

        public SecretKeyContext businessSecretKey() {
            return businessSecretKey;
        }

        public int reconnectAttempt() {
            return reconnectAttempt;
        }
    }

    /** 业务通道连接结果。 */
    public static final class Business extends ClientChannelHandoff {
        private final ClientSession targetSession;

        public Business(long stamp, SocketChannel channel, ClientSession targetSession) {
            super(stamp, channel);
            this.targetSession = Objects.requireNonNull(targetSession, "targetSession");
        }

        public ClientSession targetSession() {
            return targetSession;
        }
    }
}
