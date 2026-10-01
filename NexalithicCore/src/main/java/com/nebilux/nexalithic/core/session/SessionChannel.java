package com.nebilux.nexalithic.core.session;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.rate.DynamicRateController;
import com.nebilux.nexalithic.core.infra.rate.RateLimiter;
import com.nebilux.nexalithic.core.io.channel.LoopChannel;
import com.nebilux.nexalithic.core.io.codec.assembler.PacketsAssembler;
import com.nebilux.nexalithic.core.io.codec.fragmenter.PacketsFragmenter;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.security.SecurityCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.concurrent.atomic.LongAdder;

/**
 * 会话通道
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SessionChannel<P extends AbstractPacket, S extends NexalithicSession<S>> extends LoopChannel<SessionLoop<?, ? super SessionChannel<P, S>>, SocketChannel> {
    private static final Logger logger = LoggerFactory.getLogger(SessionChannel.class);
    protected final AbstractPacket.PacketType channelType;
    protected final S ownerSession;
    protected final PacketsFragmenter<P> fragmenter;
    protected final PacketsAssembler<P> assembler;
    protected final RateLimiter rateLimiter = new RateLimiter(1024 * 1024);
    protected final DynamicRateController.RateState rateState = new DynamicRateController.RateState();
    protected final LongAdder readBytesWindow = new LongAdder();
    protected final LongAdder writeBytesWindow = new LongAdder();
    protected final SecurityCodec securityCodec;
    protected LoopBuffer readPlainBuffer, writeCipheBuffer;
    protected LoopBuffer readCipheBuffer, writePlainBuffer;

    public SessionChannel(SecretKeyContext secretKeyContext, AbstractPacket.PacketType channelType,
                          S ownerSession, SessionLoop<?, ? super SessionChannel<P, S>> ownerLoop,
                          PacketsFragmenter<P> fragmenter, PacketsAssembler<P> assembler) {
        super(ownerLoop);
        this.channelType = channelType;
        this.ownerSession = ownerSession;
        this.fragmenter = fragmenter;
        this.assembler = assembler;
        securityCodec = new SecurityCodec(secretKeyContext);
    }

    public void updateReadRate(long rate) {
        rateLimiter.updateReadRate(rate);
        ownerLoop.postRateUpdate(this);
    }
    public void updateWriteRate(long rate) {
        rateLimiter.updateWriteRate(rate);
        ownerLoop.postRateUpdate(this);
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
            if (!ownerLoop.inEventLoop()) {
                throw new IllegalStateException(
                        String.format("Thread safety violation: [Session-%s] read/write must be performed in LoopThread. Current thread: %s",
                                ownerSession.getSessionKey(), Thread.currentThread().getName()));
            }
            readPlainBuffer = ownerLoop.aquireLoopBuffer();
            writeCipheBuffer = ownerLoop.aquireLoopBuffer();
        }
        rateLimiter.refillWriteCredit();
        long writeCredit = rateLimiter.getWriteCredit();
        if (writeCredit <= 0) {
            return 0;
        }
        boolean progressed;
        do {
            progressed = fragmenter.drain(readPlainBuffer);
            if (securityCodec.encrypt(readPlainBuffer, writeCipheBuffer)) {
                progressed = true;
            }
        } while (progressed);
        long written = writeCipheBuffer.writeToChannel(getSelectableChannel(), writeCredit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) writeCredit);
        rateLimiter.consumeWrite(written);
        if (written > 0) {
            writeBytesWindow.add(written);
        }
        if (writeCipheBuffer.isEmpty() && fragmenter.isEmpty()) {
            readPlainBuffer.recycle();
            readPlainBuffer = null;
            writeCipheBuffer.recycle();
            writeCipheBuffer = null;
            updateInterest(SelectionKey.OP_WRITE, false);
            if (!fragmenter.isEmpty()) {
                updateInterest(SelectionKey.OP_WRITE, true);
            }
        }
        return written;
    }
    public final long read() throws IOException, InvalidAlgorithmParameterException, IllegalBlockSizeException, ShortBufferException, BadPaddingException, InvalidKeyException {
        if (readCipheBuffer == null) {
            if (!ownerLoop.inEventLoop()) {
                throw new IllegalStateException(
                        String.format("Thread safety violation: [Session-%s] read/write must be performed in LoopThread. Current thread: %s",
                                ownerSession.getSessionKey(), Thread.currentThread().getName()));
            }
            readCipheBuffer = ownerLoop.aquireLoopBuffer();
            writePlainBuffer = ownerLoop.aquireLoopBuffer();
        }
        rateLimiter.refillReadCredit();
        long readCredit = rateLimiter.getReadCredit();
        if (readCredit <= 0) {
            return 0;
        }
        long read = readCipheBuffer.readFromChannel(getSelectableChannel(), readCredit > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) readCredit);
        if (read < 0) {
            return -1;
        }
        boolean progressed;
        do {
            progressed = securityCodec.decrypt(readCipheBuffer, writePlainBuffer);
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

    public final S ownerSession() {
        return ownerSession;
    }
    public final SessionLoop<?, ? super SessionChannel<P, S>> ownerLoop() {
        return ownerLoop;
    }

    public final AbstractPacket.PacketType getChannelType() {
        return channelType;
    }

    @Override
    protected void onClose(Transport<SocketChannel> transport) {
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
        rateState.reset();
    }
}
