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

package com.nebilux.nexalithic.core.io.codec.assembler;

import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.connection.session.SessionKey;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.internal.model.stream.StreamCodecAccess;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.StreamFeeder;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodec;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodecRegistry;

import java.io.IOException;

/**
 * 业务流汇编程序
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class BusinessStreamAssembler extends BusinessModelAssembler<BusinessStream<?>> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, BusinessStreamAssembler.class);
    public static final class Options extends BusinessModelAssembler.Options {
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

    public BusinessStreamAssembler(SessionKey sessionKey, int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory,
                                   long maxIdleTimeNanos, ChunkCodecRegistry chunkCodecRegistry, TimeWheel<CodecContext<?>> timeWheel) {
        super(sessionKey, maxOutstandingContextCount, codecCallbackFactory, maxIdleTimeNanos, timeWheel);
        this.chunkCodecRegistry = chunkCodecRegistry;
    }

    @Override
    protected boolean deliverOnFirstFrame() {
        return true;
    }

    @Override
    protected CodecContext<BusinessStream<?>> createCodecContext(int modelSequence, BusinessModel.Way way, short[] path, long correlationId, LoopBuffer input, int surplus) throws IOException {
        if (surplus < EXTRA_HEADER_SIZE) {
            throw new IOException("Stream model header is incomplete");
        }
        try {
            BusinessStream.Direction direction = BusinessStream.Direction.from(input.unsafeGetByte()).opposite();
            BusinessStream.Policy.Sequence sequence = BusinessStream.Policy.Sequence.from(input.unsafeGetByte());
            BusinessStream.Policy.Buffering remoteInput = readBuffering(input);
            BusinessStream.Policy.Buffering remoteOutput = readBuffering(input);
            BusinessStream.Policy policy = new BusinessStream.Policy(sequence, remoteOutput, remoteInput);
            int codecId = input.unsafeGetInt();
            ChunkCodec<?> codec = chunkCodecRegistry.get(codecId);
            if (codec == null) {
                throw new IOException("Unknown Stream chunk codec: " + codecId);
            }
            return createTypedContext(modelSequence, codec, direction, policy, way, path, correlationId, outputAvailableCallback());
        } catch (IllegalArgumentException | IllegalStateException failure) {
            throw new IOException("Invalid Stream START payload", failure);
        }
    }

    private static BusinessStream.Policy.Buffering readBuffering(LoopBuffer input) {
        int maxChunks = input.unsafeGetInt();
        long maxBytes = input.unsafeGetLong();
        BusinessStream.Policy.OnFull onFull = BusinessStream.Policy.OnFull.from(input.unsafeGetByte());
        return new BusinessStream.Policy.Buffering(maxChunks, maxBytes, onFull);
    }

    private <C extends BusinessStream.Chunk> CodecContext<BusinessStream<?>> createTypedContext(
            int modelSequence, ChunkCodec<C> codec, BusinessStream.Direction direction,
            BusinessStream.Policy policy, BusinessModel.Way way, short[] path, long correlationId, Runnable readRetry) {
        return new StreamCodecContext<>(modelSequence, codecCallbackFactory.create(modelSequence, correlationId, way),
                BusinessStream.open(correlationId, way, path, direction, policy, codec.clazz()), codec, readRetry);
    }

    private static final class StreamCodecContext<C extends BusinessStream.Chunk> extends CodecContext<BusinessStream<?>> {
        private final BusinessStream<?> stream;
        private final StreamCodecAccess<C> access;
        private final ChunkCodec.Decoder<C> decoder;
        private final Runnable readRetry;
        private C pendingChunk;
        private int currentChunkRemaining = -1;

        private StreamCodecContext(int modelSequence, BusinessModelCodecCallback callback, BusinessStream<C> model, ChunkCodec<C> codec, Runnable readRetry) {
            super(modelSequence, callback);
            this.stream = model;
            this.decoder = codec.decoder();
            this.access = StreamCodecAccess.from(model);
            this.readRetry = readRetry;
        }

        @Override
        protected void readPayload(LoopBuffer input, int quota) throws IOException {
            if (quota > 0 && !stream.direction().hasOutput()) {
                throw new IOException("Stream without output received chunk data");
            }
            if (!flushPendingChunk()) {
                return;
            }
            while (quota > 0) {
                if (currentChunkRemaining < 0) {
                    if (quota < CHUNK_METADATA_SIZE) {
                        throw new IOException("Incomplete chunk metadata in Stream frame");
                    }
                    long sequence = input.unsafeGetLong();
                    int length = input.unsafeGetInt();
                    if (length < 0) {
                        throw new IOException("Negative encoded chunk length: " + length);
                    }
                    decoder.prepare(sequence, length);
                    currentChunkRemaining = length;
                    quota -= CHUNK_METADATA_SIZE;
                }
                if (currentChunkRemaining > 0) {
                    int consumed = decode(decoder, currentChunkRemaining, input, quota);
                    if (consumed == 0) {
                        throw new IOException("Chunk decoder made no progress");
                    }
                    quota -= consumed;
                    currentChunkRemaining -= consumed;
                }
                if (currentChunkRemaining == 0) {
                    C chunk = decoder.tryFinish();
                    if (chunk == null) {
                        throw new IOException("Chunk decoder did not finish at the declared length");
                    }
                    decoder.reset();
                    currentChunkRemaining = -1;
                    if (access.offerOutputChunk(chunk) == StreamFeeder.OfferResult.RETRY) {
                        pendingChunk = chunk;
                        if (readRetry != null) {
                            access.onOutputAvailable(readRetry);
                        }
                        return;
                    }
                }
            }
        }

        @Override
        protected boolean tryCompleteDecoding() throws IOException {
            if (currentChunkRemaining >= 0) {
                throw new IOException("Stream END before the current chunk is complete");
            }
            return flushPendingChunk();
        }

        private boolean flushPendingChunk() {
            if (pendingChunk == null) {
                return true;
            }
            if (access.offerOutputChunk(pendingChunk) == StreamFeeder.OfferResult.RETRY) {
                return false;
            }
            pendingChunk = null;
            access.onOutputAvailable(null);
            return true;
        }

        @Override
        public BusinessStream<?> model() {
            return stream;
        }

        @Override
        protected void onComplete() {
            if (stream.direction().hasOutput()) {
                access.finishOutput();
            }
            onDiscard();
        }

        @Override
        protected void onDiscard() {
            pendingChunk = null;
            decoder.clear();
            access.onOutputAvailable(null);
        }
    }
}
