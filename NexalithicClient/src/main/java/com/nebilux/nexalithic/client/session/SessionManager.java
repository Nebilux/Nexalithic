package com.nebilux.nexalithic.client.session;

import com.nebilux.nexalithic.client.NexalithicClient;
import com.nebilux.nexalithic.client.io.session.ClientChannelHandoff;
import com.nebilux.nexalithic.client.io.session.ClientSessionLoop;
import com.nebilux.nexalithic.client.security.ClientSecurityPolicy;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.event.EventDefinition;
import com.nebilux.nexalithic.core.event.EventTopic;
import com.nebilux.nexalithic.core.event.NexalithicEvent;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.loop.ChannelLoop;
import com.nebilux.nexalithic.core.model.packet.signaling.channel.ChannelAccessRequestSignal;
import com.nebilux.nexalithic.core.model.packet.signaling.channel.ChannelAccessResponseSignal;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.security.SecretKeyUtils;
import com.nebilux.nexalithic.core.security.SecurityPolicy;
import com.nebilux.nexalithic.core.session.SessionChannel;
import com.nebilux.nexalithic.core.session.SessionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.util.EnumMap;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 管理客户端 Session 的建立、恢复、终止及其连接路由。
 *
 * <p>SessionManager 负责阻塞式连接与安全握手、连接代际、业务通道协商和 Session 状态；
 * 已接入 Channel 的 Selector 所有权仍由 {@link ClientSessionLoop} 持有。</p>
 *
 * @author Reonvia
 * @since 0.2.2
 */
public class SessionManager {
    /** Session 状态事件。 */
    public static final class Events extends EventDefinition {
        /**
         * SessionManager 状态转换事件。
         *
         * @param from              转换前状态
         * @param to                转换后状态
         * @param terminationReason 已建立 Session 的终止原因；未发生 Session 终止时为 {@code null}
         * @param attachment        与本次转换有关的附加信息，例如目标地址、底层断开原因或失败异常
         */
        public record StateTransition(State from, State to, TerminationReason terminationReason, Object attachment) implements NexalithicEvent {}

        private final EventTopic<StateTransition> stateTransitionTopic;

        private Events(EventTopic<StateTransition> stateTransitionTopic) {
            this.stateTransitionTopic = stateTransitionTopic;
        }
    }

    /**
     * 当前受管理 Session 的生命周期状态。
     */
    public enum State {
        /** 当前没有可用 Session，也没有正在进行的恢复操作。 */
        INACTIVE,
        /** 正在建立新的 Session。 */
        ESTABLISHING,
        /** Session 已建立并可供使用。 */
        ACTIVE,
        /** 不再接受新工作，等待当前 Session 的任务与待写数据排空。 */
        DRAINING,
        /** 原 Session 已终止，正在尝试建立替代 Session。 */
        RECOVERING
    }
    /**
     * 已建立 Session 的终止原因。
     *
     * <p>该枚举描述的是整个逻辑 Session 的终止，而不是某个物理 Channel 的断开。
     * Session 尚未建立时发生的连接或握手失败不属于 Session 终止，因此不会产生该原因。</p>
     */
    public enum TerminationReason {
        /** 用户或上层组件主动终止 Session。 */
        LOCAL_REQUEST,
        /** 对端请求终止 Session，或者对端已经关闭承载 Session 的连接。 */
        REMOTE_TERMINATION,
        /** Session 因底层连接不可恢复地丢失而终止。 */
        CONNECTION_LOST,
        /** Session 因违反协议或协议状态异常而终止。 */
        PROTOCOL_FAILURE,
        /** Session 因身份验证、密钥协商或数据认证失败而终止。 */
        SECURITY_FAILURE,
        /** 当前 Session 被新的 Session 替代。 */
        REPLACED,
        /** SessionManager 或所属 Endpoint 正在终止。 */
        MANAGER_TERMINATING,
        /** 框架内部错误导致 Session 无法继续运行。 */
        INTERNAL_FAILURE
    }
    /**
     * 当前发布的 Session 及其连接 stamp。
     *
     * <p>Session 与 stamp 必须作为一个不可分割的快照发布，避免旧 Session 在并发
     * 断开后读取并采用已经推进的新 stamp。Session 为 {@code null} 时表示当前 stamp
     * 尚未发布 Session，或者已经撤销原 Session。</p>
     */
    private record SessionPair(ClientSession session, long stamp) {
        /**
         * 创建下一个连接 stamp 的空 Session 快照。
         *
         * <p>推进 stamp 即表示当前连接身份失效，因此新快照不会保留原 Session。</p>
         */
        private SessionPair next() {
            return new SessionPair(null, stamp + 1);
        }

        /**
         * 在当前连接 stamp 上发布指定 Session。
         *
         * <p>该操作表示当前连接从“尚未发布 Session”进入“Session 已发布”状态，
         * 因此只替换快照中的 Session，不推进 stamp。{@code expectedStamp} 通常来自
         * 完成握手的 Handoff；只有它仍与当前快照匹配时，安装结果才属于当前连接。</p>
         *
         * <p>重复安装同一个 Session 是幂等的，直接返回当前快照；如果当前 stamp
         * 已经发布了另一个 Session，则拒绝覆盖，避免在同一连接身份下出现两个所有者。</p>
         *
         * @param candidate     等待发布的 Session
         * @param expectedStamp 发起安装操作时观察到的连接 stamp
         * @return 已经安装同一 Session 时返回当前快照，否则返回保持当前 stamp 的新快照
         * @throws NullPointerException       candidate 为 {@code null}
         * @throws RejectedExecutionException expectedStamp 已经过期
         * @throws IllegalStateException      当前 stamp 已经发布了另一个 Session
         */
        private SessionPair install(ClientSession candidate, long expectedStamp) {
            Objects.requireNonNull(candidate, "candidate");
            if (expectedStamp != stamp) {
                throw new RejectedExecutionException("Session belongs to a stale connection stamp");
            }
            if (session == candidate) {
                return this;
            }
            if (session != null) {
                throw new IllegalStateException("A different client session is already active");
            }
            return new SessionPair(candidate, stamp);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(SessionManager.class);
    private static final int MAX_RECONNECT_ATTEMPTS = 5;
    private final AtomicReference<State> state = new AtomicReference<>(State.INACTIVE);
    /** 当前唯一已经接入的客户端 Session 及其连接 stamp。 */
    private final AtomicReference<SessionPair> sessionPair = new AtomicReference<>(new SessionPair(null, 0));
    private final Events events;
    private final Object connectionLock = new Object();
    private final AtomicBoolean accepting = new AtomicBoolean(false);
    /** 只在 connectionLock 下修改；关闭期间禁止新连接和客户端提交。 */
    private volatile boolean terminationRequested;
    /** 只在 connectionLock 下访问；stop() 完成权只领取一次。 */
    private boolean stopped;
    /**
     * 按类型保存当前打开流程的目标 Channel；同一类型非空时拒绝重复打开。
     *
     * <p>映射结构在构造时一次性建立，运行期间不再改变；每个类型使用独立的
     * AtomicReference，使不同类型的附属 Channel 可以并行打开。</p>
     */
    private final EnumMap<NexalithicChannel.Kind, AtomicReference<SessionChannel<?, ClientSession>>> channelOpenTargets = createChannelOpenTargets();
    /** 执行连接和退避等待；全部核心线程空闲后均可回收。 */
    private final ThreadPoolExecutor executor = createExecutor();
    private final ClientSecurityPolicy securityPolicy;
    private volatile ClientSessionLoop sessionLoop;
    private volatile InetSocketAddress serverAddress;

    private static ThreadPoolExecutor createExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2, 2, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                Thread.ofPlatform().daemon(true).name("Nexalithic-Client-Session-", 0).factory()
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    private static EnumMap<NexalithicChannel.Kind, AtomicReference<SessionChannel<?, ClientSession>>> createChannelOpenTargets() {
        EnumMap<NexalithicChannel.Kind, AtomicReference<SessionChannel<?, ClientSession>>> targets = new EnumMap<>(NexalithicChannel.Kind.class);
        for (NexalithicChannel.Kind kind : NexalithicChannel.Kind.values()) {
            targets.put(kind, new AtomicReference<>());
        }
        return targets;
    }

    public SessionManager(NexalithicBuilderContext context) {
        securityPolicy = context.getModule(NexalithicClient.MODULES.SecurityPolicy);
        NexalithicEventBus eventBus = context.getModule(NexalithicClient.MODULES.EventBus);
        events = new Events(eventBus.registerTopic(Events.StateTransition.class));
    }

    /** 在 Builder 装配阶段绑定唯一的 SessionLoop。 */
    public synchronized void bindSessionLoop(ClientSessionLoop sessionLoop) {
        if (this.sessionLoop != null) {
            throw new IllegalStateException("ClientSessionLoop has already been bound");
        }
        this.sessionLoop = Objects.requireNonNull(sessionLoop, "sessionLoop");
    }

    /** 允许提交新的 Session 建立操作。生命周期只允许启动一次。 */
    public void start() {
        synchronized (connectionLock) {
            if (terminationRequested || executor.isShutdown()) {
                throw new IllegalStateException("SessionManager has already been terminated");
            }
            accepting.set(true);
        }
    }

    /**
     * 请求优雅关闭：拒绝新工作，保留当前 Session 处理已接受的任务和待写数据。
     * 无当前 Session 时直接撤销尚未发布的连接尝试。
     */
    public void shutdown() {
        boolean draining;
        synchronized (connectionLock) {
            if (terminationRequested) {
                return;
            }
            terminationRequested = true;
            accepting.set(false);
            abortChannelOpenings();
            draining = sessionPair.get().session() != null;
            if (draining) {
                transition(State.DRAINING, null, null);
            } else {
                clearSessionAndAdvance();
                clearServerAddressAndTransitionToInactive(null, null);
            }
        }
        executor.shutdownNow();
        if (draining) {
            sessionLoop().wakeup();
        }
    }

    /**
     * 立即终止。可在优雅关闭期间调用，将等待排空升级为直接关闭。
     */
    public void stop() {
        try {
            synchronized (connectionLock) {
                if (stopped) {
                    return;
                }
                stopped = true;
                terminationRequested = true;
                accepting.set(false);
                SessionPair previous = clearSessionAndAdvance();
                abortChannelOpenings();
                ClientSession current = previous.session();
                try {
                    if (current != null) {
                        current.close();
                    }
                } finally {
                    clearServerAddressAndTransitionToInactive(
                            current != null ? TerminationReason.MANAGER_TERMINATING : null, null
                    );
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /** 是否已经请求优雅关闭或立即终止。 */
    public boolean isShuttingDown() {
        return terminationRequested;
    }

    /**
     * 仅由 SessionLoop 线程调用。当前任务及本地待写数据排空后关闭 Session。
     * 这不保证对端已经收到或处理了数据。
     */
    public void tryFinishGracefulShutdown() {
        if (!sessionLoop().inEventLoop()) {
            throw new IllegalStateException("Graceful shutdown must be checked on the SessionLoop thread");
        }
        if (!terminationRequested || getState() != State.DRAINING) {
            return;
        }
        synchronized (connectionLock) {
            if (stopped || getState() != State.DRAINING) {
                return;
            }
            ClientSession current = sessionPair.get().session();
            if (current == null || current.getTaskCoordinator().hasActiveTasks()
                    || !current.getSignalingChannel().isWriteDrained()
                    || !current.getBusinessChannel().isWriteDrained()) {
                return;
            }
            clearSessionAndAdvance();
            try {
                current.close();
            } finally {
                clearServerAddressAndTransitionToInactive(TerminationReason.MANAGER_TERMINATING, null);
            }
        }
    }

    /**
     * 在调用线程中建立信令连接并完成安全握手，随后把结果交给 SessionLoop。
     */
    public boolean connect(InetSocketAddress remote) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, ShortBufferException {
        Objects.requireNonNull(remote, "remote");
        long attemptStamp;
        synchronized (connectionLock) {
            if (!isAccepting()) {
                throw new IllegalStateException("SessionManager is not running");
            }
            SessionManager.State state = getState();
            if (state != SessionManager.State.INACTIVE) {
                throw new IllegalStateException(
                        "Cannot establish a session while in State " + state + ", must be " + SessionManager.State.INACTIVE
                );
            }
            SessionPair previous = sessionPair.get();
            if (previous.session() != null) {
                throw new IllegalStateException("A client session is already active");
            }
            SessionPair establishing = previous.next();
            if (!sessionPair.compareAndSet(previous, establishing)) {
                throw new IllegalStateException("Client session state changed while establishing a connection");
            }
            attemptStamp = establishing.stamp();
            setServerAddress(remote);
            transition(SessionManager.State.ESTABLISHING, null, remote);
        }

        logger.info("Linking to [{}]", remote);
        Throwable failure = null;
        try {
            boolean submitted = establishSignaling(remote, attemptStamp, 0);
            if (!submitted) {
                abortConnectionAttempt(attemptStamp, null);
            }
            return submitted;
        } catch (Throwable throwable) {
            failure = throwable;
            throw throwable;
        } finally {
            if (failure != null) {
                abortConnectionAttempt(attemptStamp, failure);
            }
        }
    }

    /**
     * 主动断开当前逻辑连接。后台操作通过 stamp 失效，已经排队的旧 Handoff 也不会重新接入。
     */
    public void disconnect() {
        ClientSession current;
        synchronized (connectionLock) {
            SessionPair previous = clearSessionAndAdvance();
            abortChannelOpenings();
            current = previous.session();
            clearServerAddressAndTransitionToInactive(current != null || getState() == SessionManager.State.ACTIVE ? SessionManager.TerminationReason.LOCAL_REQUEST : null, null);
        }
        if (current != null) {
            current.close();
        }
    }

    /** 判断 SessionManager 当前是否接受新的连接结果。 */
    public boolean isAccepting() {
        return accepting.get();
    }

    /** 判断指定 stamp 是否属于当前逻辑连接。 */
    public boolean isCurrent(long expectedStamp) {
        return sessionPair.get().stamp() == expectedStamp;
    }

    /** 判断指定 Session 和 stamp 是否属于当前逻辑连接。 */
    public boolean isCurrent(ClientSession expectedSession, long expectedStamp) {
        SessionPair current = sessionPair.get();
        return current.session() == expectedSession && current.stamp() == expectedStamp;
    }

    /** 判断当前是否仍接受指定连接 stamp 产生的异步结果。 */
    private boolean accepts(long expectedStamp) {
        return isAccepting() && isCurrent(expectedStamp);
    }

    /** 判断当前是否仍接受指定 Session 和 stamp 产生的异步结果。 */
    private boolean accepts(ClientSession expectedSession, long expectedStamp) {
        return isAccepting() && isCurrent(expectedSession, expectedStamp);
    }

    public State getState() {
        return state.get();
    }

    /** 返回当前已经接入的唯一客户端 Session；尚未建立时返回 {@code null}。 */
    public ClientSession getCurrentSession() {
        return sessionPair.get().session();
    }

    public boolean requestChannelAccess(SessionChannel<?, ClientSession> targetChannel) {
        ClientSession ownerSession = targetChannel.getOwnerSession();
        if (terminationRequested || !isAccepting() || getCurrentSession() != ownerSession) {
            return false;
        }
        return ownerSession.pushSignalingPacket(new ChannelAccessRequestSignal(targetChannel.getKind()));
    }

    /** 接收当前 Session 返回的完整通道接入信息。 */
    public void onChannelAccess(ClientSession session, ChannelAccessResponseSignal access) {
        if (access.getKind() != NexalithicChannel.Kind.Packet_Business) {
            throw new IllegalArgumentException(
                    "Unsupported channel access response kind: " + access.getKind()
            );
        }
        SessionChannel<?, ClientSession> targetChannel = session.getChannel(access.getKind());
        AtomicReference<SessionChannel<?, ClientSession>> openTarget = channelOpenTarget(access.getKind());
        SessionPair current = sessionPair.get();
        if (terminationRequested || !isAccepting() || current.session() != session) {
            targetChannel.abortOpen();
            return;
        }
        if (!openTarget.compareAndSet(null, targetChannel)) {
            return;
        }
        long attemptStamp = current.stamp();
            if (terminationRequested || !accepts(session, attemptStamp)) {
            abortChannelOpening(targetChannel);
            return;
        }
        try {
            SessionKey token = access.getToken();
            InetSocketAddress address = access.resolve(session.getSignalingChannel().getRemoteAddress().getAddress());
            executor.execute(() -> establishBusiness(session, targetChannel, address, token, attemptStamp));
        } catch (RejectedExecutionException failure) {
            abortChannelOpening(targetChannel);
            logger.debug("Business connection rejected because SessionManager is stopping", failure);
        } catch (Error error) {
            abortChannelOpening(targetChannel);
            abortActiveSession(session, attemptStamp, error);
            throw error;
        }
    }

    /** 接收 SessionLoop 发布的成功接管通知。 */
    public void onChannelAcquired(ClientChannelHandoff handoff, SessionChannel<?, ClientSession> channel) {
        synchronized (connectionLock) {
            ClientSession ownerSession = channel.getOwnerSession();
            switch (handoff) {
                case ClientChannelHandoff.Signaling signaling -> {
                    if (!accepts(signaling.stamp())) {
                        throw new RejectedExecutionException("Channel belongs to a stale connection stamp");
                    }
                    installSession(ownerSession, signaling.stamp());
                    transition(SessionManager.State.ACTIVE, null, null);
                    logger.info("Session establishment succeeded");
                }
                case ClientChannelHandoff.Business business -> {
                    if (!accepts(ownerSession, business.stamp())) {
                        throw new RejectedExecutionException("Business channel belongs to a stale client session");
                    }
                    releaseChannelOpenTarget(channel);
                }
                default -> throw new RejectedExecutionException("Unknown handoff: " + handoff);
            }
        }
    }

    /** 接收 SessionLoop 发布的接管失败通知。 */
    public void onChannelAcquireFailed(ClientChannelHandoff handoff, Throwable failure) {
        switch (handoff) {
            case ClientChannelHandoff.Signaling signaling -> {
                if (!accepts(signaling.stamp())) {
                    return;
                }
                if (signaling.reconnectAttempt() > 0) {
                    InetSocketAddress remote = serverAddress;
                    if (remote == null) {
                        abortConnectionAttempt(signaling.stamp(), failure);
                        return;
                    }
                    scheduleSignalingAttempt(remote, signaling.stamp(), signaling.reconnectAttempt() + 1, reconnectDelayMillis(signaling.reconnectAttempt()), failure);
                } else {
                    abortConnectionAttempt(signaling.stamp(), failure);
                }
            }
            case ClientChannelHandoff.Business business -> {
                ClientSession targetSession = business.targetSession();
                SessionChannel<?, ClientSession> targetChannel = targetSession.getChannel(NexalithicChannel.Kind.Packet_Business);
                abortChannelOpening(targetChannel);
                if (!accepts(targetSession, business.stamp())) {
                    return;
                }
                if (failure instanceof Error fatal) {
                    abortActiveSession(targetSession, business.stamp(), fatal);
                    return;
                }
                logger.warn("Business channel acquisition failed", failure);
                scheduleBusinessNegotiation(targetSession);
            }
            default -> throw new RejectedExecutionException("Unknown handoff: " + handoff);
        }
    }

    public void onChannelDisconnected(SessionChannel<?, ClientSession> channel, ChannelLoop.Event.Disconnect.Reason reason) {
        ClientSession ownerSession = channel.getOwnerSession();
        switch (channel.getKind()) {
            case Packet_Signaling -> {
                ownerSession.close();
                synchronized (connectionLock) {
                    SessionPair cleared = removeSessionAndAdvance(ownerSession);
                    if (cleared == null) {
                        return;
                    }
                    abortChannelOpenings();
                    if (!isAccepting() || reason == ChannelLoop.Event.Disconnect.Reason.LOOP_TERMINATING) {
                        clearServerAddressAndTransitionToInactive(SessionManager.TerminationReason.MANAGER_TERMINATING, reason);
                        return;
                    }
                    if (reason == ChannelLoop.Event.Disconnect.Reason.LOCAL_REQUEST) {
                        clearServerAddressAndTransitionToInactive(SessionManager.TerminationReason.LOCAL_REQUEST, reason);
                        return;
                    }
                    if (!shouldReconnect(reason)) {
                        clearServerAddressAndTransitionToInactive(mapTerminationReason(reason), reason);
                        return;
                    }
                    InetSocketAddress remote = serverAddress;
                    if (remote == null) {
                        clearServerAddressAndTransitionToInactive(mapTerminationReason(reason), reason);
                        return;
                    }
                    long reconnectStamp = cleared.stamp();
                    transition(SessionManager.State.RECOVERING, mapTerminationReason(reason), reason);
                    scheduleSignalingAttempt(remote, reconnectStamp, 1, 0, null);
                }
            }
            case Packet_Business -> {
                releaseChannelOpenTarget(channel);
                if (!isAccepting() || getCurrentSession() != ownerSession || !shouldReconnect(reason)) {
                    return;
                }
                scheduleBusinessNegotiation(ownerSession);
            }
            default -> throw new RejectedExecutionException("Unknown channel kind: " + channel.getKind());
        }
    }

    private boolean establishSignaling(InetSocketAddress remote, long attemptStamp, int reconnectAttempt) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, ShortBufferException {
        SocketChannel socketChannel = null;
        ClientChannelHandoff.Signaling handoff = null;
        try {
            socketChannel = SocketChannel.open(remote);
            socketChannel.configureBlocking(true);
            handoff = performSignalingHandshake(socketChannel, attemptStamp, reconnectAttempt);
            socketChannel = null;
            if (!accepts(attemptStamp) || !sessionLoop().submitAcquireEvent(handoff)) {
                handoff.discard();
                return false;
            }
            handoff = null;
            return true;
        } finally {
            if (handoff != null) {
                handoff.discard();
            }
            closeQuietly(socketChannel);
        }
    }
    private ClientChannelHandoff.Signaling performSignalingHandshake(SocketChannel socketChannel, long attemptStamp, int reconnectAttempt) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, ShortBufferException {
        writeFully(socketChannel, ByteBuffer.allocate(SecurityPolicy.MAGIC_NUMBER_LENGTH).putLong(SecurityPolicy.MAGIC_NUMBER).flip());
        MessageDigest transcriptHash = SecretKeyUtils.createTranscriptHash();
        int certificatesLength = securityPolicy.certificatesLength();
        int keyAndSignatureLength = SecretKeyUtils.ECDH_LENGTH + securityPolicy.signatureLength();
        int serverHelloLength = certificatesLength + keyAndSignatureLength;
        int serverFinishedLength = SecretKeyUtils.FINISHED_LENGTH + SessionKey.LENGTH + SecretKeyContext.TAG_LENGTH * 2;
        ByteBuffer readBuffer = ByteBuffer.allocate(Math.max(serverHelloLength, serverFinishedLength));
        readBuffer.limit(serverHelloLength);
        readFully(socketChannel, readBuffer);
        readBuffer.flip();
        transcriptHash.update(readBuffer.asReadOnlyBuffer());
        securityPolicy.certificatesFormBuffer(readBuffer.slice(0, certificatesLength));
        if (!securityPolicy.verify(readBuffer.slice(certificatesLength, keyAndSignatureLength))) {
            throw new SecurityException("NexalithicCertificate verification failed");
        }
        KeyPair keyPair = SecretKeyUtils.generateKeyPair();
        ByteBuffer writeBuffer = ByteBuffer.allocate(
                SecretKeyUtils.ECDH_LENGTH + SecretKeyUtils.FINISHED_LENGTH + SecretKeyContext.TAG_LENGTH
        );
        writeBuffer.put(SecretKeyUtils.rawPublickey(keyPair.getPublic()));
        transcriptHash.update(writeBuffer.asReadOnlyBuffer().flip());
        byte[] secret = SecretKeyUtils.compactSecret(keyPair.getPrivate(), readBuffer.slice(certificatesLength, keyAndSignatureLength));
        byte[] localFinished = SecretKeyUtils.generateFinished(secret, transcriptHash.digest());
        SecretKeyContext signalingSecretKey = SecretKeyUtils.generateSessionSecretKey(
                secret, SecretKeyUtils.LABEL_CLIENT_SIGNALING, SecretKeyUtils.LABEL_SERVER_SIGNALING
        );
        writeBuffer.put(signalingSecretKey.encrypt(localFinished));
        writeFully(socketChannel, writeBuffer.flip());
        readBuffer.clear().limit(serverFinishedLength);
        readFully(socketChannel, readBuffer);
        readBuffer.flip();
        int encryptedFinishedLength = SecretKeyUtils.FINISHED_LENGTH + SecretKeyContext.TAG_LENGTH;
        byte[] remoteFinished = signalingSecretKey.decrypt(readBuffer.slice(0, encryptedFinishedLength));
        if (!MessageDigest.isEqual(localFinished, remoteFinished)) {
            throw new SecurityException("Finished verification failed");
        }
        ByteBuffer sessionKeyBuffer = ByteBuffer.allocate(SessionKey.LENGTH);
        signalingSecretKey.decrypt(
                readBuffer.slice(encryptedFinishedLength, SessionKey.LENGTH + SecretKeyContext.TAG_LENGTH),
                sessionKeyBuffer
        );
        SecretKeyContext businessSecretKey = SecretKeyUtils.generateSessionSecretKey(
                secret, SecretKeyUtils.LABEL_CLIENT_BUSINESS, SecretKeyUtils.LABEL_SERVER_BUSINESS
        );
        return new ClientChannelHandoff.Signaling(
                attemptStamp, socketChannel, new SessionKey.Immutable(sessionKeyBuffer.flip(), 0),
                signalingSecretKey, businessSecretKey, reconnectAttempt
        );
    }

    private void establishBusiness(ClientSession targetSession, SessionChannel<?, ClientSession> targetChannel, InetSocketAddress target, SessionKey token, long attemptStamp) {
        SocketChannel socketChannel = null;
        ClientChannelHandoff.Business handoff = null;
        try {
            if (!accepts(targetSession, attemptStamp)) {
                abortChannelOpening(targetChannel);
                return;
            }
            socketChannel = SocketChannel.open(target);
            socketChannel.configureBlocking(true);
            writeFully(socketChannel, ByteBuffer.allocate(SecurityPolicy.MAGIC_NUMBER_LENGTH).putLong(SecurityPolicy.MAGIC_NUMBER).flip());
            ByteBuffer tokenBuffer = ByteBuffer.allocate(SessionKey.LENGTH);
            token.toByteBuffer(tokenBuffer);
            writeFully(socketChannel, tokenBuffer.flip());
            handoff = new ClientChannelHandoff.Business(attemptStamp, socketChannel, targetSession);
            socketChannel = null;
            if (!accepts(targetSession, attemptStamp) || !sessionLoop().submitAcquireEvent(handoff)) {
                handoff.discard();
                handoff = null;
                throw new RejectedExecutionException("ClientSessionLoop is not accepting business channels");
            }
            handoff = null;
        } catch (Exception failure) {
            abortChannelOpening(targetChannel);
            if (accepts(targetSession, attemptStamp)) {
                logger.warn("Business channel connection failed", failure);
                scheduleBusinessNegotiation(targetSession);
            }
        } catch (Error fatal) {
            abortChannelOpening(targetChannel);
            abortActiveSession(targetSession, attemptStamp, fatal);
            throw fatal;
        } finally {
            if (handoff != null) {
                try {
                    handoff.discard();
                } catch (Throwable discardFailure) {
                    logger.debug("Failed to discard business channel handoff", discardFailure);
                }
            }
            closeQuietly(socketChannel);
        }
    }

    private void scheduleSignalingAttempt(InetSocketAddress remote, long reconnectStamp, int attempt, long delayMillis, Throwable previousFailure) {
        if (!accepts(reconnectStamp)) {
            return;
        }
        if (attempt > MAX_RECONNECT_ATTEMPTS) {
            logger.error("All {} signaling reconnection attempts failed", MAX_RECONNECT_ATTEMPTS, previousFailure);
            abortConnectionAttempt(reconnectStamp, previousFailure);
            return;
        }
        try {
            executeDelayed(() -> executeSignalingAttempt(remote, reconnectStamp, attempt, previousFailure), delayMillis);
        } catch (RejectedExecutionException failure) {
            if (isAccepting()) {
                abortConnectionAttempt(reconnectStamp, failure);
            }
        }
    }
    private void executeSignalingAttempt(InetSocketAddress remote, long reconnectStamp, int attempt, Throwable previousFailure) {
        if (!accepts(reconnectStamp)) {
            return;
        }
        logger.debug("Signaling reconnection attempt [{}/{}] to {}", attempt, MAX_RECONNECT_ATTEMPTS, remote);
        try {
            if (!establishSignaling(remote, reconnectStamp, attempt)) {
                scheduleSignalingAttempt(remote, reconnectStamp, attempt + 1, reconnectDelayMillis(attempt), previousFailure);
            }
        } catch (Exception failure) {
            logger.warn("Signaling reconnection attempt [{}/{}] failed", attempt, MAX_RECONNECT_ATTEMPTS, failure);
            scheduleSignalingAttempt(remote, reconnectStamp, attempt + 1, reconnectDelayMillis(attempt), failure);
        } catch (Error fatal) {
            logger.error("Signaling reconnection attempt [{}/{}] terminated by a fatal error", attempt, MAX_RECONNECT_ATTEMPTS, fatal);
            abortConnectionAttempt(reconnectStamp, fatal);
            throw fatal;
        }
    }

    private void scheduleBusinessNegotiation(ClientSession targetSession) {
        if (!isAccepting() || getCurrentSession() != targetSession) {
            return;
        }
        try {
            executeDelayed(() -> executeBusinessNegotiation(targetSession), TimeUnit.SECONDS.toMillis(1));
        } catch (RejectedExecutionException failure) {
            logger.debug("Business reconnection rejected because SessionManager is stopping", failure);
        }
    }
    private void executeBusinessNegotiation(ClientSession targetSession) {
        if (isAccepting() && getCurrentSession() == targetSession) {
            targetSession.tryOpenChannel(NexalithicChannel.Kind.Packet_Business);
        }
    }

    private void executeDelayed(Runnable action, long delayMillis) {
        executor.execute(() -> {
            if (delayMillis > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(delayMillis);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            action.run();
        });
    }

    /**
     * 转换当前 Session 状态并在转换成功后发布事件。
     *
     * <p>多个线程可以同时请求状态转换。只有成功改变状态的线程会发布事件；转换到当前
     * 已有状态时不会重复发布。{@code terminationReason} 只应用于已建立 Session 的终止，
     * 建立失败或恢复失败应通过 {@code attachment} 传递原始失败。</p>
     */
    private void transition(State targeted, TerminationReason terminationReason, Object attachment) {
        Objects.requireNonNull(targeted, "targeted");
        while (true) {
            State previous = state.get();
            if (previous == targeted) {
                return;
            }
            if (!state.compareAndSet(previous, targeted)) {
                Thread.onSpinWait();
                continue;
            }
            if (events.stateTransitionTopic.isSubscribed()) {
                events.stateTransitionTopic.publish(new Events.StateTransition(previous, targeted, terminationReason, attachment));
            }
            return;
        }
    }

    /**
     * 返回指定 Channel 类型的连接建立槽位。
     *
     * <p>同一类型的所有线程通过同一个 AtomicReference 竞争打开权；不同类型使用
     * 相互独立的槽位，不会彼此阻塞。</p>
     */
    private AtomicReference<SessionChannel<?, ClientSession>> channelOpenTarget(NexalithicChannel.Kind kind) {
        return Objects.requireNonNull(channelOpenTargets.get(kind), "Unknown channel kind: " + kind);
    }

    /**
     * 仅在目标仍是本次 Channel 时释放对应类型的打开权。
     *
     * @return 是否由本次调用释放
     */
    private boolean releaseChannelOpenTarget(SessionChannel<?, ClientSession> targetChannel) {
        return channelOpenTarget(targetChannel.getKind()).compareAndSet(targetChannel, null);
    }

    /**
     * 仅在指定 Channel 仍持有对应类型的打开权时释放槽位并回退打开状态。
     *
     * <p>CAS 失败表示该打开权已经由其他路径释放或转交，本方法不会再修改 Channel，
     * 避免迟到的失败结果中止后续打开流程。</p>
     */
    private void abortChannelOpening(SessionChannel<?, ClientSession> targetChannel) {
        if (releaseChannelOpenTarget(targetChannel)) {
            targetChannel.abortOpen();
        }
    }

    /**
     * 中止所有尚未完成的附属 Channel 打开流程。
     *
     * <p>已经在工作线程或事件队列中的旧结果仍可能返回，但 Session stamp 检查会阻止
     * 它们重新接入。</p>
     */
    private void abortChannelOpenings() {
        for (AtomicReference<SessionChannel<?, ClientSession>> openTarget : channelOpenTargets.values()) {
            SessionChannel<?, ClientSession> targetChannel = openTarget.getAndSet(null);
            if (targetChannel != null) {
                targetChannel.abortOpen();
            }
        }
    }

    /**
     * 中止尚未发布 Session 的连接尝试。
     *
     * <p>仅当 Manager 仍接受连接、当前尚无 Session 且 stamp 匹配时领取处理权；
     * 否则由已经改变连接身份的路径负责收尾。领取后推进 stamp，使旧异步结果失效，
     * 清除服务器地址并转换到 INACTIVE。没有已发布的 Session，因此终止原因留空，
     * {@code failure} 作为状态事件的附件；无异常的提交失败允许传入 {@code null}。</p>
     */
    private void abortConnectionAttempt(long expectedStamp, Throwable failure) {
        synchronized (connectionLock) {
            SessionPair current = sessionPair.get();
            if (!isAccepting() || current.session() != null || current.stamp() != expectedStamp) {
                return;
            }
            if (!sessionPair.compareAndSet(current, current.next())) {
                return;
            }
            clearServerAddressAndTransitionToInactive(null, failure);
        }
    }

    /**
     * 因致命错误中止已经发布的当前 Session。
     *
     * <p>仅当 Manager 仍接受连接、当前 Session 与 stamp 均匹配时领取处理权；
     * 否则由已经改变连接身份的路径负责收尾。领取后推进 stamp，使旧异步结果失效，
     * 中止附属 Channel 的打开流程，并先关闭 Session 的逻辑身份，再清除服务器地址、
     * 转换到 INACTIVE。状态事件使用 INTERNAL_FAILURE 作为终止原因，以 {@code failure}
     * 作为附件；底层 Channel 的物理关闭仍由所属 Loop 完成。</p>
     */
    private void abortActiveSession(ClientSession failedSession, long expectedStamp, Error failure) {
        synchronized (connectionLock) {
            SessionPair current = sessionPair.get();
            if (!isAccepting() || current.session() != failedSession || current.stamp() != expectedStamp || !sessionPair.compareAndSet(current, current.next())) {
                return;
            }
            abortChannelOpenings();
            try {
                failedSession.close();
            } catch (Throwable closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            clearServerAddressAndTransitionToInactive(SessionManager.TerminationReason.INTERNAL_FAILURE, failure);
        }
    }

    private void clearServerAddressAndTransitionToInactive(TerminationReason terminationReason, Object attachment) {
        serverAddress = null;
        transition(SessionManager.State.INACTIVE, terminationReason, attachment);
    }

    private static boolean shouldReconnect(ChannelLoop.Event.Disconnect.Reason reason) {
        return reason == ChannelLoop.Event.Disconnect.Reason.IO_FAILURE || reason == ChannelLoop.Event.Disconnect.Reason.IDLE_TIMEOUT || reason == ChannelLoop.Event.Disconnect.Reason.INTERNAL_FAILURE;
    }

    private static SessionManager.TerminationReason mapTerminationReason(ChannelLoop.Event.Disconnect.Reason reason) {
        return switch (reason) {
            case LOCAL_REQUEST -> SessionManager.TerminationReason.LOCAL_REQUEST;
            case LOOP_TERMINATING -> SessionManager.TerminationReason.MANAGER_TERMINATING;
            case REMOTE_REQUEST, REMOTE_CLOSED -> SessionManager.TerminationReason.REMOTE_TERMINATION;
            case IO_FAILURE, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, TRANSFER_FAILURE -> SessionManager.TerminationReason.CONNECTION_LOST;
            case PROTOCOL_VIOLATION -> SessionManager.TerminationReason.PROTOCOL_FAILURE;
            case SECURITY_FAILURE -> SessionManager.TerminationReason.SECURITY_FAILURE;
            case CHANNEL_REPLACED -> SessionManager.TerminationReason.REPLACED;
            case INTERNAL_FAILURE -> SessionManager.TerminationReason.INTERNAL_FAILURE;
        };
    }

    private void setServerAddress(InetSocketAddress serverAddress) {
        this.serverAddress = Objects.requireNonNull(serverAddress, "serverAddress");
    }

    /**
     * 发布新接入的 Session。
     *
     * <p>该方法在信令 Channel 已经由 SessionLoop 登记所有权后调用。重复发布同一对象
     * 是幂等的，但在旧 Session 尚未移除时拒绝安装另一个 Session。</p>
     */
    private void installSession(ClientSession candidate, long expectedStamp) {
        while (true) {
            SessionPair current = sessionPair.get();
            SessionPair installed = current.install(candidate, expectedStamp);
            if (installed == current) {
                return;
            }
            if (sessionPair.compareAndSet(current, installed)) {
                return;
            }
            Thread.onSpinWait();
        }
    }

    /** 原子撤销任意当前 Session 并推进连接 stamp。 */
    private SessionPair clearSessionAndAdvance() {
        while (true) {
            SessionPair current = sessionPair.get();
            SessionPair cleared = current.next();
            if (sessionPair.compareAndSet(current, cleared)) {
                return current;
            }
            Thread.onSpinWait();
        }
    }

    /** 仅在指定 Session 仍为当前 Session 时撤销它并推进连接 stamp。 */
    private SessionPair removeSessionAndAdvance(ClientSession expectedSession) {
        while (true) {
            SessionPair current = sessionPair.get();
            if (current.session() != expectedSession) {
                return null;
            }
            SessionPair cleared = current.next();
            if (sessionPair.compareAndSet(current, cleared)) {
                return cleared;
            }
            Thread.onSpinWait();
        }
    }

    private ClientSessionLoop sessionLoop() {
        ClientSessionLoop loop = sessionLoop;
        if (loop == null) {
            throw new IllegalStateException("ClientSessionLoop has not been bound");
        }
        return loop;
    }

    private static long reconnectDelayMillis(int completedAttempt) {
        return 3_000L * completedAttempt;
    }

    private static void readFully(SocketChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining()) {
            if (channel.read(target) < 0) {
                throw new EOFException("Remote peer closed during client handshake");
            }
        }
    }

    private static void writeFully(SocketChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) {
            channel.write(source);
        }
    }

    private static void closeQuietly(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException failure) {
            logger.debug("Failed to close client socket", failure);
        }
    }
}
