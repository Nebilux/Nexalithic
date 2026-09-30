package com.nebilux.nexalithic.server.io.handshake;

import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.SelfStaticRecyclableWrapper;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.loop.ChannelLoop;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.session.SessionKey;
import com.nebilux.nexalithic.server.session.ServerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 单次服务端握手使用的池化上下文。
 *
 * <p>上下文从对象池获取后临时拥有一个 {@link SocketChannel}，保存握手阶段、
 * I/O 缓冲区、密码学中间状态和握手结果。握手成功后，物理连接以及会话材料
 * 通过 {@code takeXxx()} 方法一次性移交给目标 SessionLoop；握手失败时，
 * 上下文负责关闭仍由自己持有的连接。</p>
 *
 * <h2>生命周期</h2>
 * <ol>
 *     <li>从对象池获取上下文。</li>
 *     <li>调用 {@link #init(NexalithicChannel.Kind, SocketChannel)} 绑定连接。</li>
 *     <li>由 HandshakeLoop 注册 SelectionKey 并执行握手。</li>
 *     <li>成功时调用 {@link #takeChannel()} 以及相应的 {@code takeXxx()} 方法。</li>
 *     <li>无论成功或失败，最终调用 {@link #discard()} 归还对象池。</li>
 * </ol>
 *
 * <p>{@link #close()} 只关闭当前物理连接，不归还对象池；
 * {@link #discard()} 才结束本次租用并触发回收。</p>
 *
 * <h2>线程约束</h2>
 * <p>缓冲区、握手阶段和密码学字段应由所属 HandshakeLoop 串行访问。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/27
 */
public class HandshakeContext extends SelfStaticRecyclableWrapper<HandshakeContext> implements NexalithicChannel, ChannelLoop.Handoff {
    /**
     * 握手处理阶段。
     */
    public enum Phase {

        /** 上下文位于对象池中，尚未绑定连接。 */
        IDLE,

        /** 正在读取协议魔数。 */
        READ_MAGIC,

        /** 正在异步生成服务端证书、公钥和签名。 */
        PREPARE_SERVER_HELLO,

        /** 正在发送服务端证书、公钥和签名。 */
        WRITE_SERVER_HELLO,

        /** 正在读取客户端公钥和 Finished 数据。 */
        READ_CLIENT_HANDSHAKE,

        /** 正在校验客户端并派生会话密钥。 */
        VERIFY_CLIENT_HANDSHAKE,

        /** 正在发送服务端 Finished 和 SessionKey。 */
        WRITE_SERVER_FINISH,

        /** 正在读取通道关联令牌。 */
        READ_CHANNEL_TOKEN,

        /** 握手完成，等待移交给目标 SessionLoop。 */
        READY,

        /**
         * 已经请求回收，但仍有一个已提交的异步任务持有 Context。
         * 异步任务不得继续计算，只需通知 HandshakeLoop 可以完成回收。
         */
        RECYCLE_PENDING;

        public boolean isAsync() {
            return this == PREPARE_SERVER_HELLO || this == VERIFY_CLIENT_HANDSHAKE;
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(HandshakeContext.class);
    private final AtomicReference<NexalithicChannel.State> state = new AtomicReference<>(NexalithicChannel.State.Closed);
    private final HandshakeLoop ownerLoop;
    private final ByteBuffer readBuffer;
    private final ByteBuffer writeBuffer;
    /*
     * transport 字段可能被关闭线程、Selector 线程和目标 Loop 读取。
     */
    private volatile SocketChannel socketChannel;
    private volatile SelectionKey selectionKey;
    private volatile InetSocketAddress remoteAddress;
    private volatile NexalithicChannel.Kind kind;
    private volatile Phase phase = Phase.IDLE;
    private volatile long lastActiveTimeNanos = -1L;
    private volatile Throwable asyncFailure;
    /*
     * 握手内部状态。
     *
     * 应只由 HandshakeLoop 修改。异步密码任务应将结果提交回
     * HandshakeLoop，而不是直接并发修改这些字段。
     */
    private PrivateKey privateKey;
    /*
     * 信令通道握手结果。
     */
    private SessionKey sessionKey;
    private SecretKeyContext signalingSecretContext;
    private SecretKeyContext businessSecretContext;
    /*
     * 业务通道握手结果。
     */
    private ServerSession targetSession;

    public HandshakeContext(GenericWrapperPool<HandshakeContext, HandshakeContext> owner, HandshakeLoop ownerLoop, int readBufferCapacity, int writeBufferCapacity) {
        super(owner);
        this.ownerLoop = ownerLoop;
        this.readBuffer = ByteBuffer.allocate(readBufferCapacity);
        this.writeBuffer = ByteBuffer.allocate(writeBufferCapacity);
    }

    /**
     * 将本次对象池租用绑定到一个新连接。
     *
     * <p>该方法不配置非阻塞模式，也不注册 SelectionKey，这些操作由
     * HandshakeLoop 完成。</p>
     *
     * <p>初始化失败时不会关闭 {@code channel}，因为 HandshakeIngress
     * 尚未成功接收连接，连接所有权仍属于调用方。</p>
     *
     * @param kind    通道种类
     * @param channel 已由 Acceptor 接受的 SocketChannel
     * @return 当前上下文
     * @throws IOException 无法读取远端地址
     */
    public HandshakeContext init(NexalithicChannel.Kind kind, SocketChannel channel) throws IOException {
        if (!isActive()) {
            throw new IllegalStateException("HandshakeContext must be acquired before initialization");
        }
        if (phase != Phase.IDLE) {
            throw new IllegalStateException("HandshakeContext is not idle: " + phase);
        }
        if (!state.compareAndSet(NexalithicChannel.State.Closed, NexalithicChannel.State.Opening)) {
            throw new IllegalStateException("Cannot initialize HandshakeContext from channel state " + state.get());
        }
        this.kind = kind;
        this.socketChannel = channel;
        this.remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
        this.phase = Phase.READ_MAGIC;
        this.lastActiveTimeNanos = System.nanoTime();
        readBuffer.clear();
        writeBuffer.clear();
        state.set(NexalithicChannel.State.Opened);
        return this;
    }

    /**
     * 一次性移出物理连接。
     *
     * <p>调用成功后，连接所有权转移给调用方；后续回收当前 Context
     * 不会再关闭该 SocketChannel。</p>
     */
    public SocketChannel takeChannel() {
        if (phase != Phase.READY) {
            throw new IllegalStateException(
                    "Handshake is not ready for transfer: " + phase
            );
        }
        SocketChannel result = socketChannel;
        if (result == null) {
            throw new IllegalStateException("HandshakeContext does not own a SocketChannel");
        }
        socketChannel = null;
        return result;
    }
    /**
     * 一次性移出信令握手生成的 SessionKey。
     */
    public SessionKey takeSessionKey() {
        if (phase != Phase.READY) {
            throw new IllegalStateException(
                    "Handshake is not ready for transfer: " + phase
            );
        }
        SessionKey result = sessionKey;
        if (result == null) {
            throw new IllegalStateException(
                    "HandshakeContext does not contain a SessionKey"
            );
        }
        sessionKey = null;
        return result;
    }
    /**
     * 一次性移出信令通道密钥上下文。
     */
    public SecretKeyContext takeSignalingSecretContext() {
        if (phase != Phase.READY) {
            throw new IllegalStateException(
                    "Handshake is not ready for transfer: " + phase
            );
        }
        SecretKeyContext result = signalingSecretContext;
        if (result == null) {
            throw new IllegalStateException(
                    "HandshakeContext does not contain a signaling secret context"
            );
        }
        signalingSecretContext = null;
        return result;
    }
    /**
     * 一次性移出业务通道密钥上下文。
     */
    public SecretKeyContext takeBusinessSecretContext() {
        if (phase != Phase.READY) {
            throw new IllegalStateException(
                    "Handshake is not ready for transfer: " + phase
            );
        }
        SecretKeyContext result = businessSecretContext;
        if (result == null) {
            throw new IllegalStateException(
                    "HandshakeContext does not contain a business secret context"
            );
        }
        businessSecretContext = null;
        return result;
    }
    /**
     * 一次性移出业务通道对应的既有 Session。
     */
    public ServerSession takeTargetSession() {
        if (phase != Phase.READY) {
            throw new IllegalStateException(
                    "Handshake is not ready for transfer: " + phase
            );
        }
        ServerSession result = targetSession;
        if (result == null) {
            throw new IllegalStateException(
                    "HandshakeContext does not contain a target session"
            );
        }
        targetSession = null;
        return result;
    }

    public NexalithicChannel.Kind getKind() {
        return kind;
    }
    public Phase getPhase() {
        return phase;
    }
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }
    public SelectionKey getSelectionKey() {
        return selectionKey;
    }
    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }
    public ByteBuffer getWriteBuffer() {
        return writeBuffer;
    }
    public HandshakeLoop ownerLoop() {
        return ownerLoop;
    }

    void setPhase(Phase phase) {
        this.phase = phase;
    }

    /**
     * 绑定由 HandshakeLoop 注册的 SelectionKey。
     */
    void attachSelectionKey(SelectionKey newSelectionKey) {
        if (state.get() != NexalithicChannel.State.Opened) {
            throw new IllegalStateException(
                    "Cannot attach SelectionKey while channel state is " + state.get()
            );
        }
        selectionKey = newSelectionKey;
        newSelectionKey.attach(this);
    }

    /**
     * Selector 重建时替换 SelectionKey。
     */
    void replaceSelectionKey(SelectionKey replacement) {
        selectionKey = replacement;
        replacement.attach(this);
    }

    void setPrivateKey(PrivateKey privateKey) {
        this.privateKey = privateKey;
    }
    PrivateKey getPrivateKey() {
        return privateKey;
    }
    void setSessionMaterial(SessionKey sessionKey, SecretKeyContext signalingSecretContext, SecretKeyContext businessSecretContext) {
        this.sessionKey = sessionKey;
        this.signalingSecretContext = signalingSecretContext;
        this.businessSecretContext = businessSecretContext;
    }
    HandshakeContext setTargetSession(ServerSession targetSession) {
        this.targetSession = targetSession;
        return this;
    }

    void setAsyncFailure(Throwable failure) {
        asyncFailure = failure;
    }

    Throwable takeAsyncFailure() {
        Throwable failure = asyncFailure;
        asyncFailure = null;
        return failure;
    }

    @Override
    public NexalithicChannel.State getState() {
        return state.get();
    }

    @Override
    public void updateLastActiveTimeNanos(long lastActiveTimeNanos) {
        this.lastActiveTimeNanos = lastActiveTimeNanos;
    }

    @Override
    public long getLastActiveTimeNanos() {
        return lastActiveTimeNanos;
    }

    /**
     * 关闭当前仍由 Context 持有的物理连接。
     *
     * <p>该方法不会回收 Context。</p>
     */
    @Override
    public boolean close() throws IOException {
        while (true) {
            NexalithicChannel.State current = state.get();
            if (current == NexalithicChannel.State.Closed || current == NexalithicChannel.State.Closing) {
                return false;
            }
            if (state.compareAndSet(current, NexalithicChannel.State.Closing)) {
                break;
            }
        }
        SelectionKey closingKey = selectionKey;
        selectionKey = null;
        SocketChannel closingChannel = socketChannel;
        socketChannel = null;
        lastActiveTimeNanos = -1L;
        if (closingKey != null) {
            closingKey.cancel();
        }
        try {
            if (closingChannel != null) {
                closingChannel.close();
            }
        } finally {
            state.set(NexalithicChannel.State.Closed);
        }
        return true;
    }

    /**
     * 丢弃本次交接并归还对象池。
     *
     * <p>实际的关闭和字段清理由 {@link #onReset()} 统一完成。</p>
     */
    @Override
    public void discard() {
        recycle();
    }

    /**
     * 对象归还池前的统一清理入口。
     */
    @Override
    protected void onReset() {
        try {
            close();
        } catch (IOException exception) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to close handshake channel during reset", exception);
            }
        }
        wipe(readBuffer);
        wipe(writeBuffer);
        kind = null;
        phase = Phase.IDLE;
        remoteAddress = null;
        privateKey = null;
        sessionKey = null;
        signalingSecretContext = null;
        businessSecretContext = null;
        targetSession = null;
        asyncFailure = null;
    }

    @Override
    public String toString() {
        return "HandshakeContext[" +
                "kind=" + kind +
                ", phase=" + phase +
                ", state=" + state.get() +
                ", remoteAddress=" + remoteAddress +
                ']';
    }

    /**
     * 擦除池化缓冲区中的握手数据。
     */
    private static void wipe(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            int from = buffer.arrayOffset();
            int to = from + buffer.capacity();
            Arrays.fill(buffer.array(), from, to, (byte) 0);
        } else {
            for (int index = 0; index < buffer.capacity(); index++) {
                buffer.put(index, (byte) 0);
            }
        }
        buffer.clear();
    }
}