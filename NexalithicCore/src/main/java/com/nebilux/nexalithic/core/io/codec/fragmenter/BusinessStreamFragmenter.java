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

package com.nebilux.nexalithic.core.io.codec.fragmenter;

import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.internal.model.stream.StreamCodecAccess;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
import com.nebilux.nexalithic.core.io.codec.CodecFrame;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodec;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodecRegistry;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 业务流碎片器。多线程提交的流先在提交线程包装为上下文并进入 MPSC 队列，
 * 再由所属 Loop 线程轮流编码。调用方必须保证 {@link #feed(BusinessStream)}
 * 与 {@link #clear()} 不并发。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class BusinessStreamFragmenter extends BusinessModelFragmenter<BusinessStream<?>> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, BusinessStreamFragmenter.class);
    public static class Options extends BusinessModelFragmenter.Options {
        private Options(Class<?> holder) {
            super(holder);
        }

        @Override
        protected int MaxOutstandingContextCount_Value() {
            return 16;
        }
    }

    private static final int EXTRA_HEADER_SIZE = Byte.BYTES // Direction
            + Byte.BYTES // Policy.Sequence
            + 2 * (Integer.BYTES // maxChunks
                    + Long.BYTES // maxBytes
                    + Byte.BYTES // OnFull
            ) // 输入和输出 Buffering
            + Integer.BYTES; // ChunkCodec ID
    private static final int CHUNK_METADATA_SIZE = Long.BYTES // Chunk 序号
            + Integer.BYTES; // 编码长度

    private final ChunkCodecRegistry chunkCodecRegistry;
    private volatile Runnable outputAvailableCallback;

    public BusinessStreamFragmenter(ChunkCodecRegistry chunkCodecRegistry, int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory) {
        super(maxOutstandingContextCount, codecCallbackFactory);
        this.chunkCodecRegistry = chunkCodecRegistry;
    }

    public void onOutputAvailable(Runnable callback) {
        outputAvailableCallback = Objects.requireNonNull(callback, "outputAvailableCallback");
    }

    @Override
    protected CodecContext<BusinessStream<?>> createCodecContext(BusinessStream<?> model, int modelSequence) {
        return createTypedContext(model, modelSequence);
    }

    @SuppressWarnings("unchecked")
    private <C extends BusinessStream.Chunk> CodecContext<BusinessStream<?>> createTypedContext(BusinessStream<C> model, int modelSequence) {
        ChunkCodec<?> codec = chunkCodecRegistry.get(model.type());
        if (codec == null) {
            throw new IllegalStateException("No codec for type " + model.type());
        }
        if (codec.clazz() != model.type()) {
            throw new IllegalStateException("Codec type does not match " + model.type());
        }
        return new StreamCodecContext<>(modelSequence, codecCallbackFactory.create(modelSequence, model.getCorrelationId(), model.getWay()), model, (ChunkCodec<C>) codec);
    }

    private final class StreamCodecContext<C extends BusinessStream.Chunk> extends CodecContext<BusinessStream<?>> {
        private final StreamCodecAccess<C> access;
        private final ChunkCodec.Encoder<C> encoder;
        private final int codecId;
        private final boolean hasInput;
        private final AtomicBoolean inputReady = new AtomicBoolean(true);
        private C currentChunk;
        private int chunkRemaining;
        private boolean isComplete;

        private StreamCodecContext(int modelSequence, BusinessModelCodecCallback codecCallback, BusinessStream<C> model, ChunkCodec<C> codec) {
            super(modelSequence, codecCallback, model);
            codecId = codec.id();
            encoder = codec.encoder();
            access = StreamCodecAccess.from(model);
            hasInput = model.direction().hasInput();
            if (hasInput) {
                access.onInputAvailable(() -> {
                    inputReady.set(true);
                    signalReadyOutput();
                    Runnable callback = outputAvailableCallback;
                    if (callback != null) {
                        callback.run();
                    }
                });
            }
        }

        @Override
        protected int extraHeaderSize() {
            return EXTRA_HEADER_SIZE;
        }

        @Override
        protected void writeExtraFirstFrame(LoopBuffer output) {
            output.put(model().direction().code());
            BusinessStream.Policy policy = model().policy();
            output.put(policy.sequence().code());
            writeBuffering(output, policy.input());
            writeBuffering(output, policy.output());
            output.put(codecId);
        }

        @Override
        protected void onFirstFrameCommitted(int advanced) {
            if (!hasInput) {
                isComplete = true;
            }
        }

        @Override
        protected void writeNextFrame(LoopBuffer output, int quota) throws IOException {
            if (!hasInput) {
                return;
            }
            int total = 0;
            while (total < quota) {
                if (currentChunk == null) {
                    if (quota - total < CHUNK_METADATA_SIZE * MIN_HEADER_COUNT) {
                        break;
                    }
                    inputReady.set(false);
                    C chunk = access.pollInputChunk();
                    if (chunk == null) {
                        break;
                    }
                    inputReady.set(true);
                    int length = encoder.prepare(chunk);
                    if (length < 0) {
                        throw new IOException("Negative encoded chunk length: " + length);
                    }
                    currentChunk = chunk;
                    chunkRemaining = length;
                    output.put(currentChunk.sequence());
                    output.put(chunkRemaining);
                    total += CHUNK_METADATA_SIZE;
                }
                LoopBuffer.LimitedWritableView view = output.unsafeLimitedWritableView(Math.min(chunkRemaining, quota - total));
                int before = view.remaining();
                encoder.encode(view);
                int written = before - view.remaining();
                if (written < 0 || written > chunkRemaining) {
                    throw new IOException("Chunk encoder consumed an invalid length");
                }
                chunkRemaining -= written;
                total += written;
                if (chunkRemaining > 0) {
                    break;
                } else if (chunkRemaining == 0) {
                    if (!encoder.tryFinish()) {
                        throw new IOException("Chunk encoder did not finish at the declared length");
                    }
                    encoder.reset();
                    currentChunk = null;
                } else {
                    throw new IOException("Encoding error");
                }
            }
        }

        @Override
        protected boolean tryWriteLastFrame(LoopBuffer output) {
            if (!hasInput || currentChunk != null || !access.isInputFinished()
                    || output.writableBytes() < CodecFrame.HEADER_LENGTH) {
                return false;
            }
            output.put(CodecFrame.packEnd(modelSequence()));
            isComplete = true;
            return true;
        }

        @Override
        protected boolean isReady() {
            if (!hasInput) {
                return !isComplete;
            }
            return inputReady.get() || currentChunk != null || access.isInputFinished();
        }

        @Override
        protected boolean isComplete() {
            return isComplete;
        }

        @Override
        protected void onComplete() {
            onDiscard();
        }

        @Override
        protected void onDiscard() {
            if (hasInput) {
                access.onInputAvailable(null);
            }
            encoder.clear();
            currentChunk = null;
        }

        private void writeBuffering(LoopBuffer output, BusinessStream.Policy.Buffering buffering) {
            output.put(buffering.maxChunks());
            output.put(buffering.maxBytes());
            output.put(buffering.onFull().code());
        }
    }
}
