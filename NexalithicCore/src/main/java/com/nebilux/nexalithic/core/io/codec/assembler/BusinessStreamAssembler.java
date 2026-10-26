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

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.internal.model.stream.StreamCodecAccess;
import com.nebilux.nexalithic.core.io.codec.CodecFrame;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.StreamFeeder;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodec;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodecRegistry;
import org.jctools.queues.SpmcArrayQueue;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 业务流汇编程序
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class BusinessStreamAssembler extends BusinessModelAssembler<BusinessStream<?>> {
    private static final int COMPLETED_CAPACITY = 16;
    private static final int BUFFERING_PAYLOAD_SIZE = Integer.BYTES + Long.BYTES + Byte.BYTES;
    private static final int START_PAYLOAD_FIXED_SIZE = Long.BYTES // taskId
            + Short.BYTES // Way
            + Byte.BYTES // pathDepth
            + Byte.BYTES // Direction
            + Byte.BYTES // Policy.Sequence
            + 2 * BUFFERING_PAYLOAD_SIZE // 输入和输出 Buffering
            + Integer.BYTES; // ChunkCodec ID

    private final ChunkCodecRegistry chunkCodecRegistry;
    private final Map<Integer, CodecContext<?>> assembling = new ConcurrentHashMap<>();
    private final SpmcArrayQueue<BusinessStream<?>> completed = new SpmcArrayQueue<>(COMPLETED_CAPACITY);
    private BusinessStream<?> pending;
    private CodecContext<?> blockedContext;
    private Runnable readRetry;

    public BusinessStreamAssembler(ChunkCodecRegistry chunkCodecRegistry) {
        this.chunkCodecRegistry = chunkCodecRegistry;
    }

    /** 输出块队列恢复容量后，由所属 Loop 重新处理已缓存的输入。 */
    public void onReadRetry(Runnable callback) {
        readRetry = Objects.requireNonNull(callback, "callback");
    }

    @Override
    public boolean feed(LoopBuffer source) throws IOException {
        int initialReadable = source.readableBytes();
        synchronized (this) {
            if (pending != null) {
                if (!completed.offer(pending)) {
                    return false;
                }
                pending = null;
            }
        }
        if (blockedContext != null) {
            CodecContext<?> blocked = blockedContext;
            try {
                if (!blocked.flushPendingChunks()) {
                    return false;
                }
                blockedContext = null;
            } catch (Throwable throwable) {
                blockedContext = null;
                assembling.remove(blocked.modelId, blocked);
                blocked.clear();
                throw throwable;
            }
        }
        while (source.readableBytes() >= CodecFrame.HEADER_LENGTH) {
            source.markHead();
            long header = source.unsafeGetLong();
            CodecFrame.Type frameType;
            try {
                frameType = CodecFrame.parseFrameType(header);
            } catch (IllegalArgumentException failure) {
                throw new IOException("Invalid Stream frame type", failure);
            }
            int payloadLength = CodecFrame.parsePayloadLength(header);
            int modelId = CodecFrame.parseModelId(header);
            if (payloadLength > CodecFrame.MAX_PAYLOAD_LENGTH) {
                throw new IOException("Stream frame payload exceeds maximum: " + payloadLength);
            }
            if (source.readableBytes() < payloadLength) {
                source.resetHead();
                break;
            }
            CodecContext<?> context = assembling.get(modelId);
            try {
                if (context == null) {
                    if (!frameType.isStart()) {
                        throw new IOException("Invalid model id " + modelId);
                    }
                    context = createContext(modelId, source, payloadLength);
                    BusinessStream<?> model = context.model();
                    assembling.put(modelId, context);
                    if (!completed.offer(model)) {
                        synchronized (this) {
                            pending = model;
                        }
                        break;
                    }
                } else {
                    if (!context.nextFrame(frameType, source, payloadLength)) {
                        blockedContext = context;
                        break;
                    }
                }
                if (context.isComplete()) {
                    assembling.remove(modelId);
                }
            } catch (Throwable throwable) {
                if (context != null) {
                    assembling.remove(modelId, context);
                    if (blockedContext == context) {
                        blockedContext = null;
                    }
                    context.clear();
                }
                if (throwable instanceof RuntimeException failure) {
                    throw new IOException("Stream frame decoding failed", failure);
                }
                throw throwable;
            }
        }
        return initialReadable != source.readableBytes();
    }

    @Override
    public BusinessStream<?> drain() {
        BusinessStream<?> model = completed.poll();
        if (model != null) {
            boolean retry;
            synchronized (this) {
                retry = pending != null;
            }
            if (retry && readRetry != null) {
                readRetry.run();
            }
            return model;
        }
        synchronized (this) {
            model = pending;
            pending = null;
        }
        return model;
    }

    @Override
    public void clear() {
        blockedContext = null;
        for (CodecContext<?> context : assembling.values()) {
            context.clear();
        }
        assembling.clear();
        synchronized (this) {
            pending = null;
        }
        completed.clear();
    }

    private CodecContext<?> createContext(int modelId, LoopBuffer input, int quota) throws IOException {
        if (quota < START_PAYLOAD_FIXED_SIZE) {
            throw new IOException("Stream START payload is too short: " + quota);
        }
        long taskId = input.unsafeGetLong();
        int wayCode = input.unsafeGetShort();
        int pathDepth = input.unsafeGetByte();
        if (pathDepth > BusinessModel.MAX_PATH_DEPTH || quota != START_PAYLOAD_FIXED_SIZE + pathDepth * Short.BYTES) {
            throw new IOException("Invalid Stream START payload length: " + quota);
        }
        short[] path = new short[pathDepth];
        for (int i = 0; i < pathDepth; i++) {
            path[i] = input.unsafeGetShort();
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
            return createTypedContext(modelId, codec, direction, policy, BusinessModel.Way.from((short) wayCode), path, taskId, readRetry);
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

    private static <C extends BusinessStream.Chunk> CodecContext<C> createTypedContext(
            int modelId, ChunkCodec<C> codec, BusinessStream.Direction direction,
            BusinessStream.Policy policy, BusinessModel.Way way, short[] path, long taskId, Runnable readRetry) {
        BusinessStream<C> model = BusinessStream.open(codec.clazz(), direction, policy, way, path).setTaskId(taskId);
        return new CodecContext<>(modelId, model, codec, readRetry);
    }

    private static class CodecContext<C extends BusinessStream.Chunk> {
        private static final int CHUNK_METADATA_SIZE = Long.BYTES + Integer.BYTES;

        protected final int modelId;
        protected final BusinessStream<C> model;
        protected final StreamCodecAccess<C> access;
        protected final ChunkCodec.Decoder<C> decoder;
        private final ArrayDeque<C> pendingChunks = new ArrayDeque<>();
        private final Runnable readRetry;
        private int chunkRemaining = -1;
        private boolean waitingForCapacity;
        private boolean complete;

        protected CodecContext(int modelId, BusinessStream<C> model, ChunkCodec<C> codec, Runnable readRetry) {
            this.modelId = modelId;
            this.model = model;
            this.decoder = codec.decoder();
            if (decoder == null) {
                throw new IllegalStateException("Chunk codec has no decoder: " + codec.clazz().getName());
            }
            this.access = StreamCodecAccess.from(model);
            this.readRetry = readRetry;
        }

        protected boolean nextFrame(CodecFrame.Type type, LoopBuffer input, int quota) throws IOException {
            if (complete || !pendingChunks.isEmpty()) {
                throw new IOException("Stream frame received in an invalid state");
            }
            if (type.isEnd()) {
                if (quota != 0 || chunkRemaining >= 0) {
                    throw new IOException("Stream END before the current chunk is complete");
                }
                access.finishOutput();
                complete = true;
                return true;
            }
            if (!type.isData() || quota == 0) {
                throw new IOException("Expected Stream DATA frame");
            }

            int remaining = quota;
            while (remaining > 0) {
                if (chunkRemaining < 0) {
                    if (remaining < CHUNK_METADATA_SIZE) {
                        throw new IOException("Incomplete chunk metadata in Stream DATA frame");
                    }
                    long sequence = input.unsafeGetLong();
                    int length = input.unsafeGetInt();
                    if (length < 0) {
                        throw new IOException("Negative encoded chunk length: " + length);
                    }
                    decoder.prepare(sequence, length);
                    chunkRemaining = length;
                    remaining -= CHUNK_METADATA_SIZE;
                }
                int limit = Math.min(remaining, chunkRemaining);
                if (limit > 0) {
                    LoopBuffer.LimitedReadableView view = input.unsafeLimitedReadableView(limit);
                    int decoded = decoder.decode(view);
                    int consumed = limit - view.remaining();
                    if (decoded != consumed || consumed == 0) {
                        throw new IOException("Chunk decoder made no progress or returned an invalid length");
                    }
                    remaining -= consumed;
                    chunkRemaining -= consumed;
                }
                if (chunkRemaining == 0) {
                    C chunk = decoder.tryFinish();
                    if (chunk == null) {
                        throw new IOException("Chunk decoder did not finish at the declared length");
                    }
                    pendingChunks.addLast(chunk);
                    decoder.reset();
                    chunkRemaining = -1;
                }
            }
            return flushPendingChunks();
        }

        private boolean flushPendingChunks() {
            C chunk;
            while ((chunk = pendingChunks.peekFirst()) != null) {
                StreamFeeder.OfferResult result = access.offerOutputChunk(chunk);
                if (result == StreamFeeder.OfferResult.RETRY) {
                    if (readRetry != null && !waitingForCapacity) {
                        waitingForCapacity = true;
                        access.onOutputAvailable(readRetry);
                    }
                    return false;
                }
                pendingChunks.removeFirst();
            }
            if (waitingForCapacity) {
                access.onOutputAvailable(null);
                waitingForCapacity = false;
            }
            return true;
        }

        protected boolean isComplete() {
            return complete;
        }

        protected void clear() {
            if (waitingForCapacity) {
                access.onOutputAvailable(null);
                waitingForCapacity = false;
            }
            pendingChunks.clear();
            decoder.reset();
        }

        public BusinessStream<C> model() {
            return model;
        }
    }
}
