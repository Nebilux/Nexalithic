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
import com.nebilux.nexalithic.core.io.codec.assembler.ModelAssembler;
import com.nebilux.nexalithic.core.io.codec.fragmenter.ModelFragmenter;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.model.NexalithicModel;
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
import java.util.Objects;

/**
 * 会话通道
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class SessionChannel<M extends NexalithicModel, S extends AbstractSession<S>> extends LoopChannel<SessionLoop<?, ? super SessionChannel<M, S>>, SocketChannel> {
    protected final S ownerSession;
    protected final ModelFragmenter<M> fragmenter;
    protected final ModelAssembler<M> assembler;
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

    /**
     * 将与当前通道类型匹配的模型交给编码器。
     */
    public final boolean put(M model) {
        if (getState() == State.Closing) {
            return false;
        }
        return fragmenter.feed(Objects.requireNonNull(model, "Model cannot be null"));
    }

    /** 取出一个可交付模型；没有完整 Packet 或尚未建立的 Stream 时返回 null。 */
    public final M get() {
        return assembler.drain();
    }

    public final boolean fragmenterIsEmpty() {
        return fragmenter.isEmpty();
    }

    public final boolean hasReadyOutput() {
        return fragmenter.isReady();
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
        checkInEventLoop();
        if (readPlainBuffer == null) {
            readPlainBuffer = ownerLoop.aquireLoopBuffer();
            writeCipheBuffer = ownerLoop.aquireLoopBuffer();
        }
        long writable = availableWriteBytes();
        if (writable <= 0) {
            return 0;
        }
        boolean progressed;
        do {
            progressed = fragmenter.drain(readPlainBuffer);
            if (securityCodec.encrypt(readPlainBuffer, writeCipheBuffer)) {
                progressed = true;
            }
        } while (progressed);
        long advanced = getTransport().write(writeCipheBuffer, (int) Math.min(writable, Integer.MAX_VALUE));
        onBytesWrite(advanced);
        if (advanced > 0) {
            updateLastActiveTimeNanos(System.nanoTime());
        }
        if (readPlainBuffer.isEmpty() && writeCipheBuffer.isEmpty() && !fragmenter.isReady()) {
            readPlainBuffer.recycle();
            readPlainBuffer = null;
            writeCipheBuffer.recycle();
            writeCipheBuffer = null;
            updateInterest(SelectionKey.OP_WRITE, false);
            if (fragmenter.isReady()) {
                updateInterest(SelectionKey.OP_WRITE, true);
            }
        }
        return advanced;
    }

    public final long read() throws IOException, InvalidAlgorithmParameterException, IllegalBlockSizeException, ShortBufferException, BadPaddingException, InvalidKeyException {
        checkInEventLoop();
        if (readCipheBuffer == null) {
            readCipheBuffer = ownerLoop.aquireLoopBuffer();
            writePlainBuffer = ownerLoop.aquireLoopBuffer();
        }
        long readable = availableReadBytes();
        if (readable <= 0) {
            return 0;
        }
        long advanced = getTransport().read(readCipheBuffer, (int) Math.min(readable, Integer.MAX_VALUE));
        if (advanced < 0) {
            return -1;
        }
        onBytesRead(advanced);
        if (advanced > 0) {
            updateLastActiveTimeNanos(System.nanoTime());
        }
        processBufferedInput();
        return advanced;
    }

    /** 输出队列腾出容量后，由所属 Loop 重试已解密或已接收的缓冲数据。 */
    public final void retryBufferedRead() throws IOException, InvalidAlgorithmParameterException, IllegalBlockSizeException, ShortBufferException, BadPaddingException, InvalidKeyException {
        checkInEventLoop();
        if (readCipheBuffer != null) {
            processBufferedInput();
        }
    }

    public final S getOwnerSession() {
        return ownerSession;
    }

    /** 默认不限制传输字节数；业务通道可覆盖并返回剩余额度。 */
    protected long availableWriteBytes() {
        return Integer.MAX_VALUE;
    }

    protected long availableReadBytes() {
        return Integer.MAX_VALUE;
    }

    protected void onBytesWrite(long bytes) {}

    protected void onBytesRead(long bytes) {}

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
    }

    private void processBufferedInput() throws IOException, InvalidAlgorithmParameterException, IllegalBlockSizeException, ShortBufferException, BadPaddingException, InvalidKeyException {
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
    }

    private void checkInEventLoop() {
        if (!ownerLoop.inEventLoop()) {
            throw new IllegalStateException(
                    String.format("Thread safety violation: [Session-%s] read/write must be performed in LoopThread. Current thread: %s",
                            ownerSession.getSessionKey(), Thread.currentThread().getName()));
        }
    }
}
