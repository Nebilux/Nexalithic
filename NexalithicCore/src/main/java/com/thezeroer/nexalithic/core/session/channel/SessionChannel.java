package com.thezeroer.nexalithic.core.session.channel;

import com.thezeroer.nexalithic.core.infra.buffer.LoopBuffer;
import com.thezeroer.nexalithic.core.infra.rate.DynamicRateController;
import com.thezeroer.nexalithic.core.infra.rate.RateLimiter;
import com.thezeroer.nexalithic.core.io.codec.assembler.PacketsAssembler;
import com.thezeroer.nexalithic.core.io.codec.fragmenter.PacketsFragmenter;
import com.thezeroer.nexalithic.core.io.loop.ChannelLoop;
import com.thezeroer.nexalithic.core.io.thread.LoopThread;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.security.SecurityChannel;
import com.thezeroer.nexalithic.core.session.NexalithicSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 会话通道
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/04
 * @version 1.0.0
 */
public abstract class SessionChannel<
        P extends AbstractPacket,
        S extends NexalithicSession<S, ?, ?>
        > extends SecurityChannel implements NexalithicChannel {
    private static final Logger logger = LoggerFactory.getLogger(SessionChannel.class);
    // 状态掩码：Bit 31 为 Dirty 位，低位存储 SelectionKey.OP_XXX
    private static final int DIRTY_BIT = 1 << 31;
    private static final int INTEREST_MASK = ~DIRTY_BIT;
    protected final S session;
    protected final AbstractPacket.PacketType type;
    protected final PacketsFragmenter<P> fragmenter;
    protected final PacketsAssembler<P> assembler;
    protected volatile ChannelLoop<?> loop;
    protected volatile SelectionKey selectionKey;
    protected volatile SocketChannel socketChannel;
    protected volatile InetSocketAddress remoteAddress;
    protected final AtomicInteger targetInterest = new AtomicInteger(0);
    protected final AtomicReference<State> state = new AtomicReference<>(State.Unconnected);
    protected final RateLimiter rateLimiter = new RateLimiter(1024 * 1024);
    protected final DynamicRateController.RateState rateState = new DynamicRateController.RateState();
    protected final LongAdder readBytesWindow = new LongAdder();
    protected final LongAdder writeBytesWindow = new LongAdder();
    protected LoopBuffer readPlainBuffer, writeCipheBuffer;
    protected LoopBuffer readCipheBuffer, writePlainBuffer;
    protected volatile long lastActiveTimeNanos = -1;

    public SessionChannel(AbstractPacket.PacketType packetType, S session, ChannelLoop<?> loop, PacketsFragmenter<P> fragmenter, PacketsAssembler<P> assembler, SecretKeyContext secretKeyContext) {
        super(secretKeyContext);
        this.session = session;
        this.type = packetType;
        this.loop = loop;
        this.fragmenter = fragmenter;
        this.assembler = assembler;
    }

    public final boolean becomeConnecting() {
        if (state.get() == State.Closing || state.get() == State.Closed) {
            return false;
        }
        return state.compareAndSet(State.Unconnected, State.Connecting);
    }
    public final SessionChannel<P, S> updateChannel(SelectionKey selectionKey) throws IOException {
        if (this.selectionKey == selectionKey) {
            return this;
        }
        if (this.selectionKey != null) {
            this.selectionKey.cancel();
        }
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException ignored) {}
        }
        this.selectionKey = selectionKey;
        this.socketChannel = (SocketChannel) selectionKey.channel();
        this.remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
        this.targetInterest.set(selectionKey.interestOps());
        this.state.set(State.Connected);
        return this;
    }
    public final SessionChannel<P, S> updateChannel(ChannelLoop<?> loop, SelectionKey selectionKey) throws IOException {
        this.loop = loop;
        return updateChannel(selectionKey);
    }

    public final boolean updateChannelInterest(int interest, boolean enable) {
        while (true) {
            int oldInterest = targetInterest.get();
            int newInterest = enable ? (oldInterest | interest) : (oldInterest & ~interest);
            // 性能优化：如果兴趣位没变且已经处于 Dirty 状态，直接返回 false
            if (newInterest == oldInterest && (oldInterest & DIRTY_BIT) != 0) {
                return false;
            }
            if (targetInterest.compareAndSet(oldInterest, newInterest | DIRTY_BIT)) {
                return (oldInterest & DIRTY_BIT) == 0;
            }
            Thread.onSpinWait();
        }
    }
    public final void applyTargetInterest() {
        while (true) {
            int oldInterest = targetInterest.get();
            if ((oldInterest & DIRTY_BIT) == 0) {
                return;
            }
            int newValue = oldInterest & INTEREST_MASK;
            if (targetInterest.compareAndSet(oldInterest, newValue)) {
                SelectionKey key = this.selectionKey;
                if (key != null) {
                    try {
                        if (key.interestOps() != newValue) {
                            key.interestOps(newValue);
                        }
                    } catch (IllegalArgumentException e) {
                        logger.error("applyTargetInterest error: {}", e.getMessage());
                    } catch (Exception ignored) {}
                }
                return;
            }
            Thread.onSpinWait();
        }
    }

    public void updateReadRate(long rate) {
        rateLimiter.updateReadRate(rate);
        if (loop != null) {
            loop.postRateUpdate(this);
        }
    }
    public void updateWriteRate(long rate) {
        rateLimiter.updateWriteRate(rate);
        if (loop != null) {
            loop.postRateUpdate(this);
        }
    }
    public final void applyRate() {
        rateLimiter.applyRate();
    }

    public final long evaluateDynamicRate(long intervalNanos, long nowNanos, DynamicRateController controller) {
        return controller.evaluateAndGetRate(readBytesWindow.sumThenReset(), intervalNanos, nowNanos, rateState);
    }
    public final void resetDynamicRateState() {
        rateState.reset();
        readBytesWindow.sumThenReset();
        writeBytesWindow.sumThenReset();
    }

    public final boolean put(P packet) {
        return fragmenter.feed(packet);
    }
    @SafeVarargs
    public final int fill(P... packets) {
        return fragmenter.fill(packets);
    }
    public final P get() {
        return assembler.drain();
    }

    public final boolean fragmenterIsEmpty() {
        return fragmenter.isEmpty();
    }

    public final long write() throws IOException, InvalidAlgorithmParameterException, ShortBufferException, IllegalBlockSizeException, BadPaddingException, InvalidKeyException {
        if (readPlainBuffer == null) {
            if (Thread.currentThread() instanceof LoopThread loopThread) {
                readPlainBuffer = loopThread.aquireLoopBuffer();
                writeCipheBuffer = loopThread.aquireLoopBuffer();
            } else {
                throw new IllegalStateException(
                        String.format("Thread safety violation: [Session-%s] read/write must be performed in LoopThread. Current thread: %s",
                                session.getSessionKey(), Thread.currentThread().getName()));
            }
        }
        rateLimiter.refillWriteCredit();
        long writeCredit = rateLimiter.getWriteCredit();
        if (writeCredit <= 0) {
            return 0;
        }
        boolean progressed;
        do {
            progressed = fragmenter.drain(readPlainBuffer);
            if (encrypt(readPlainBuffer, writeCipheBuffer)) {
                progressed = true;
            }
        } while (progressed);
        long written = writeCipheBuffer.writeToChannel(socketChannel, writeCredit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) writeCredit);
        if (written == 0 && writeCipheBuffer.isEmpty() && fragmenter.isEmpty()) {
            readPlainBuffer.recycle();
            readPlainBuffer = null;
            writeCipheBuffer.recycle();
            writeCipheBuffer = null;
            return -1;
        } else {
            rateLimiter.consumeWrite(written);
            if (written > 0) {
                writeBytesWindow.add(written);
            }
            return written;
        }
    }
    public final long read() throws IOException, InvalidAlgorithmParameterException, IllegalBlockSizeException, ShortBufferException, BadPaddingException, InvalidKeyException {
        if (readCipheBuffer == null) {
            if (Thread.currentThread() instanceof LoopThread loopThread) {
                readCipheBuffer = loopThread.aquireLoopBuffer();
                writePlainBuffer = loopThread.aquireLoopBuffer();
            } else {
                throw new IllegalStateException(
                        String.format("Thread safety violation: [Session-%s] read/write must be performed in LoopThread. Current thread: %s",
                                session.getSessionKey(), Thread.currentThread().getName()));
            }
        }
        rateLimiter.refillReadCredit();
        long readCredit = rateLimiter.getReadCredit();
        if (readCredit <= 0) {
            return 0;
        }
        long read = readCipheBuffer.readFromChannel(socketChannel, readCredit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) readCredit);
        if (read < 0) {
            return -1;
        }
        boolean progressed;
        do {
            progressed = decrypt(readCipheBuffer, writePlainBuffer);
            if (assembler.feed(writePlainBuffer)) {
                progressed = true;
            }
        } while (progressed);
        if (readCipheBuffer.isEmpty() && writePlainBuffer.isEmpty()) {
            readCipheBuffer.recycle();
            readCipheBuffer = null;
            writePlainBuffer.recycle();
            writePlainBuffer = null;
        }
        rateLimiter.consumeRead(read);
        if (read > 0) {
            readBytesWindow.add(read);
        }
        return read;
    }

    public final S session() {
        return session;
    }
    public final ChannelLoop<?> localLoop() {
        return loop;
    }
    @SuppressWarnings("unchecked")
    public final <C extends ChannelLoop<?>> C asLocalLoop() {
        return (C) loop;
    }
    public final AbstractPacket.PacketType getType() {
        return type;
    }
    public final State getState() {
        return state.get();
    }
    public final SelectionKey getSelectionKey() {
        return selectionKey;
    }
    public final InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public final void updateLastActiveTimeNanos(long lastActiveTimeNanos) {
        this.lastActiveTimeNanos = lastActiveTimeNanos;
        session.updateLastActiveTimeNanos(lastActiveTimeNanos);
    }
    @Override
    public final long getLastActiveTimeNanos() {
        return lastActiveTimeNanos;
    }

    @Override
    public final boolean closeChannel() {
        if (state.compareAndSet(State.Connected, State.Unconnected) || state.compareAndSet(State.Connecting, State.Unconnected)) {
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
            if (readPlainBuffer != null) {
                readPlainBuffer.recycle();
                readPlainBuffer = null;
            }
            if (writeCipheBuffer != null) {
                writeCipheBuffer.recycle();
                writeCipheBuffer = null;
            }
            if (readCipheBuffer != null) {
                readCipheBuffer.recycle();
                readCipheBuffer = null;
            }
            if (writePlainBuffer != null) {
                writePlainBuffer.recycle();
                writePlainBuffer = null;
            }
            fragmenter.clear();
            assembler.clear();
            remoteAddress = null;
            loop = null;
            lastActiveTimeNanos = -1;
            rateState.reset();
            return true;
        }
        if (session.getLastActiveTimeNanos() < 0) {
            state.set(State.Closed);
        }
        return false;
    }

    @Override
    public String toString() {
        return "Type: " + type + ", State: " + state + ", SocketChannel: " + socketChannel;
    }
}
