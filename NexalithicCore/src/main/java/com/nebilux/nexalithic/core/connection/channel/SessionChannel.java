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

package com.nebilux.nexalithic.core.connection.channel;

import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.connection.transport.TCP_Transport;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.rate.DynamicRateController;
import com.nebilux.nexalithic.core.infra.rate.RateLimiter;
import com.nebilux.nexalithic.core.io.codec.assembler.ModelAssembler;
import com.nebilux.nexalithic.core.io.codec.fragmenter.ModelFragmenter;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.model.NexalithicModel;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.stream.AbstractStream;
import com.nebilux.nexalithic.core.security.SecretKeyContext;
import com.nebilux.nexalithic.core.security.SecurityCodec;

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
 * 会话通道。模型由对应的编解码器处理；Stream 的块线路格式由具体编解码器定义。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SessionChannel<M extends NexalithicModel, S extends AbstractSession<S>> extends LoopChannel<SessionLoop<?, ? super SessionChannel<M, S>>, SocketChannel> {
    protected final S ownerSession;
    protected final ModelFragmenter<M> fragmenter;
    protected final ModelAssembler<M> assembler;
    protected final RateLimiter rateLimiter = new RateLimiter(1024 * 1024);
    protected final DynamicRateController.RateState rateState = new DynamicRateController.RateState();
    protected final LongAdder readBytesWindow = new LongAdder();
    protected final LongAdder writeBytesWindow = new LongAdder();
    protected final SecurityCodec securityCodec;
    protected LoopBuffer readPlainBuffer, writeCipheBuffer;
    protected LoopBuffer readCipheBuffer, writePlainBuffer;

    public SessionChannel(Kind kind, SessionLoop<?, ? super SessionChannel<M, S>> ownerLoop, S ownerSession,
                          ModelFragmenter<M> fragmenter, ModelAssembler<M> assembler, SecretKeyContext secretKeyContext) {
        super(kind, ownerLoop);
        this.ownerSession = ownerSession;
        this.fragmenter = fragmenter;
        this.assembler = assembler;
        securityCodec = new SecurityCodec(secretKeyContext);
    }

    public final void open(SelectionKey selectionKey) throws IOException {
        if (!(selectionKey.channel() instanceof SocketChannel socketChannel)) {
            throw new IllegalArgumentException("SessionChannel requires a SocketChannel");
        }
        super.open(selectionKey, new TCP_Transport(socketChannel));
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

    /**
     * 将与当前通道类型匹配的模型交给编码器。Stream 的后续块由框架内部实现提供。
     * 成功提交后，调用方仍需启用写兴趣；后续异步产生的块也需要再次唤醒写入。
     */
    public final boolean put(M model) {
        if (!accepts(model)) {
            throw new IllegalArgumentException("Model does not match channel kind " + getKind());
        }
        return fragmenter.feed(model);
    }

    /** 取出一个可交付模型；没有完整 Packet 或尚未建立的 Stream 时返回 null。 */
    public final M get() {
        return assembler.drain();
    }

    private boolean accepts(M model) {
        if (model == null || model.model() != getKind().model()) {
            return false;
        }
        return switch (getKind()) {
            case Packet_Signaling, Packet_Business -> model instanceof AbstractPacket packet
                    && packet.type() == getKind().packetType();
            case Stream_Media, Stream_File -> model instanceof AbstractStream<?> stream
                    && stream.type() == getKind().streamType() && stream.direction().hasInput();
        };
    }

    public final boolean fragmenterIsEmpty() {
        return fragmenter.isEmpty();
    }

    public final boolean hasReadyOutput() {
        return fragmenter.hasReadyOutput();
    }

    /**
     * 判断已接受的待写数据是否已经送入底层通道。
     *
     * <p>只允许所属 Loop 线程在没有并发写操作时用于优雅关闭判断；
     * 这不表示对端已经收到或处理了数据。</p>
     */
    public final boolean isWriteDrained() {
        return fragmenter.isEmpty()
                && (readPlainBuffer == null || readPlainBuffer.isEmpty())
                && (writeCipheBuffer == null || writeCipheBuffer.isEmpty());
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
        long written = getTransport().write(writeCipheBuffer, (int) Math.min(writeCredit, Integer.MAX_VALUE));
        rateLimiter.consumeWrite(written);
        if (written > 0) {
            writeBytesWindow.add(written);
            updateLastActiveTimeNanos(System.nanoTime());
        }
        if (readPlainBuffer.isEmpty() && writeCipheBuffer.isEmpty() && !fragmenter.hasReadyOutput()) {
            readPlainBuffer.recycle();
            readPlainBuffer = null;
            writeCipheBuffer.recycle();
            writeCipheBuffer = null;
            updateInterest(SelectionKey.OP_WRITE, false);
            if (fragmenter.hasReadyOutput()) {
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
        long read = getTransport().read(readCipheBuffer, (int) Math.min(readCredit, Integer.MAX_VALUE));
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
            updateLastActiveTimeNanos(System.nanoTime());
        }
        return read;
    }

    public final S getOwnerSession() {
        return ownerSession;
    }

    @Override
    protected void onClose(Binding<SocketChannel> binding) {
        super.onClose(binding);
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
        resetDynamicRateState();
    }
}
