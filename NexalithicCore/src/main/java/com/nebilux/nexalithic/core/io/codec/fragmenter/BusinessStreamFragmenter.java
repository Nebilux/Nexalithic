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

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.internal.model.stream.StreamCodecAccess;
import com.nebilux.nexalithic.core.io.codec.CodecFrame;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodec;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodecRegistry;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务流碎片器。多线程提交的流先在提交线程包装为上下文并进入 MPSC 队列，
 * 再由所属 Loop 线程轮流编码。调用方必须保证 {@link #feed(BusinessStream)}
 * 与 {@link #clear()} 不并发。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class BusinessStreamFragmenter extends BusinessModelFragmenter<BusinessStream<?>> {
    private static final Logger logger = LoggerFactory.getLogger(BusinessStreamFragmenter.class);
    private static final int CONTEXT_CAPACITY = 16;
    private static final int MIN_HEADER_COUNT = 2;
    private static final int BUFFERING_PAYLOAD_SIZE = Integer.BYTES // maxChunks
            + Long.BYTES // maxBytes
            + Byte.BYTES; // OnFull
    private static final int START_PAYLOAD_FIXED_SIZE = Long.BYTES // taskId
            + Integer.BYTES // ChunkCodec ID
            + Short.BYTES // Way
            + Byte.BYTES // pathDepth
            + Byte.BYTES // Direction
            + Byte.BYTES // Policy.Sequence
            + 2 * BUFFERING_PAYLOAD_SIZE; // 输入和输出 Buffering
    private static final int CHUNK_METADATA_SIZE = Long.BYTES + Integer.BYTES;

    private final ChunkCodecRegistry chunkCodecRegistry;
    private final MpscArrayQueue<CodecContext<?>> incoming = new MpscArrayQueue<>(CONTEXT_CAPACITY);
    /** 包含入队预留、MPSC 队列及环形队列中的上下文。 */
    private final AtomicInteger contextCount = new AtomicInteger();

    /** 以下字段仅由所属 Loop 线程访问。 */
    private CodecContext<?> pending;
    private CodecContext<?> cursor;
    private int activeCount;

    public BusinessStreamFragmenter(ChunkCodecRegistry chunkCodecRegistry) {
        this.chunkCodecRegistry = chunkCodecRegistry;
    }

    @Override
    public boolean feed(BusinessStream<?> model) {
        Objects.requireNonNull(model, "model");
        int count;
        do {
            count = contextCount.get();
            if (count >= CONTEXT_CAPACITY) {
                return false;
            }
        } while (!contextCount.compareAndSet(count, count + 1));
        try {
            if (incoming.offer(createContext(model))) {
                return true;
            }
        } catch (Throwable throwable) {
            contextCount.decrementAndGet();
            throw throwable;
        }
        contextCount.decrementAndGet();
        return false;
    }

    @Override
    public boolean drain(LoopBuffer target) throws IOException {
        int initialWritable = target.writableBytes();
        if (pending != null) {
            try {
                if (pending.firstFrame(target)) {
                    firstFrameAfter(pending, target);
                    pending = null;
                } else {
                    return false;
                }
            } catch (Throwable throwable) {
                pending = null;
                contextCount.decrementAndGet();
                throw throwable;
            }
        }
        for (CodecContext<?> context; (context = incoming.poll()) != null;) {
            try {
                if (context.firstFrame(target)) {
                    firstFrameAfter(context, target);
                } else {
                    pending = context;
                    return initialWritable != target.writableBytes();
                }
            } catch (Throwable throwable) {
                contextCount.decrementAndGet();
                throw throwable;
            }
        }
        for (int stalled = 0; cursor != null;) {
            CodecContext<?> context = cursor;
            CodecContext<?> next = context.next;
            try {
                if (context.nextFrame(target)) {
                    if (context.isComplete()) {
                        remove(context);
                        continue;
                    }
                    stalled = 0;
                } else if (context.isReady() || ++stalled >= activeCount) {
                    break;
                }
                cursor = next;
            } catch (Throwable throwable) {
                if (context.prev != null) {
                    remove(context);
                }
                throw throwable;
            }
        }
        return initialWritable != target.writableBytes();
    }

    @Override
    public boolean isEmpty() {
        return contextCount.get() == 0;
    }

    @Override
    public void clear() {
        if (pending != null) {
            pending = null;
            contextCount.decrementAndGet();
        }
        while (incoming.poll() != null) {
            contextCount.decrementAndGet();
        }
        while (cursor != null) {
            remove(cursor);
        }
    }

    private CodecContext<?> createContext(BusinessStream<?> model) {
        return createTypedContext(model);
    }

    private void firstFrameAfter(CodecContext<?> context, LoopBuffer target) throws IOException {
        context.nextFrame(target);
        if (context.isComplete()) {
            contextCount.decrementAndGet();
            if (logger.isTraceEnabled()) {
                logger.trace("drain BusinessStream[{}]", context.model);
            }
        } else {
            append(context);
        }
    }

    private void append(CodecContext<?> context) {
        if (cursor == null) {
            context.prev = context;
            context.next = context;
            cursor = context;
        } else {
            CodecContext<?> tail = cursor.prev;
            tail.next = context;
            context.prev = tail;
            context.next = cursor;
            cursor.prev = context;
        }
        activeCount++;
    }

    private void remove(CodecContext<?> context) {
        if (context.next == context) {
            cursor = null;
        } else {
            context.prev.next = context.next;
            context.next.prev = context.prev;
            if (cursor == context) {
                cursor = context.next;
            }
        }
        context.prev = null;
        context.next = null;
        activeCount--;
        contextCount.decrementAndGet();
    }

    @SuppressWarnings("unchecked")
    private <C extends BusinessStream.Chunk> CodecContext<C> createTypedContext(BusinessStream<C> model) {
        ChunkCodec<?> codec = chunkCodecRegistry.get(model.type());
        if (codec == null) {
            throw new IllegalStateException("No codec for type " + model.type());
        }
        if (codec.clazz() != model.type()) {
            throw new IllegalStateException("Codec type does not match " + model.type());
        }
        return new CodecContext<>(model, (ChunkCodec<C>) codec);
    }

    private static int headerSize(BusinessStream<?> model) {
        return START_PAYLOAD_FIXED_SIZE + Byte.toUnsignedInt(model.getPathDepth()) * Short.BYTES;
    }

    private static class CodecContext<C extends BusinessStream.Chunk> {
        private final int modelId;
        private final BusinessStream<C> model;
        private final StreamCodecAccess<C> access;
        private final ChunkCodec.Encoder<C> encoder;
        private final int codecId;
        private CodecContext<?> prev;
        private CodecContext<?> next;
        private C currentChunk;
        private int chunkRemaining;
        private boolean isComplete;

        protected CodecContext(BusinessStream<C> model, ChunkCodec<C> codec) {
            this.model = model;
            modelId = model.getModelId();
            codecId = codec.id();
            encoder = codec.encoder();
            access = StreamCodecAccess.from(model);
        }

        protected boolean firstFrame(LoopBuffer output) throws IOException {
            int header = headerSize(model);
            int required = CodecFrame.HEADER_LENGTH + header;
            if (output.writableBytes() < required) {
                return false;
            }
            long start = output.getTail();
            try {
                output.advanceTail(CodecFrame.HEADER_LENGTH);
                writeFirst(output);
                long end = output.getTail();
                if (end - start != CodecFrame.HEADER_LENGTH + header) {
                    throw new IOException("Stream firstFrame length mismatch");
                }
                output.setTail(start);
                output.put(CodecFrame.packStart(modelId, header));
                output.setTail(end);
            } catch (Throwable failure) {
                output.setTail(start);
                throw failure;
            }
            return true;
        }

        protected boolean nextFrame(LoopBuffer output) throws IOException {
            if (isComplete() || output.writableBytes() < CodecFrame.HEADER_LENGTH * MIN_HEADER_COUNT) {
                return false;
            }
            long start = output.getTail();
            try {
                output.advanceTail(CodecFrame.HEADER_LENGTH);
                int total = writeNext(output, Math.min(output.writableBytes(), CodecFrame.MAX_PAYLOAD_LENGTH));
                if (total > 0) {
                    long end = output.getTail();
                    if (end - start != CodecFrame.HEADER_LENGTH + total) {
                        throw new IOException("Stream nextFrame length mismatch");
                    }
                    output.setTail(start);
                    output.put(CodecFrame.packData(modelId, total));
                    output.setTail(end);
                    if (currentChunk == null && access.isInputFinished() && output.writableBytes() >= CodecFrame.HEADER_LENGTH) {
                        output.put(CodecFrame.packEnd(modelId));
                        isComplete = true;
                    }
                    return true;
                }
                output.setTail(start);
                if (currentChunk == null && access.isInputFinished()) {
                    output.put(CodecFrame.packEnd(modelId));
                    isComplete = true;
                    return true;
                }
                return false;
            } catch (Throwable failure) {
                output.setTail(start);
                throw failure;
            }
        }

        protected boolean isReady() {
            return false;
        }

        protected boolean isComplete() {
            return isComplete;
        }

        private void writeFirst(LoopBuffer output) {
            output.put(model.getTaskId());
            output.put(model.getWay().code());
            byte pathDepth = model.getPathDepth();
            output.put(pathDepth);
            if (pathDepth > 0) {
                for (short value : model.getPath()) {
                    output.put(value);
                }
            }
            output.put(model.direction().code());
            BusinessStream.Policy policy = model.policy();
            output.put(policy.sequence().code());
            writeBuffering(output, policy.input());
            writeBuffering(output, policy.output());
            output.put(codecId);
        }

        private int writeNext(LoopBuffer output, int quota) throws IOException {
            int total = 0;
            while (total < quota) {
                if (currentChunk == null) {
                    if (quota - total < CHUNK_METADATA_SIZE * MIN_HEADER_COUNT) {
                        break;
                    }
                    C chunk = access.pollInputChunk();
                    if (chunk == null) {
                        break;
                    }
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
                int written = encoder.encode(output.unsafeLimitedWritableView(Math.min(chunkRemaining, quota - total)));
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
            return total;
        }

        private void writeBuffering(LoopBuffer output, BusinessStream.Policy.Buffering buffering) {
            output.put(buffering.maxChunks());
            output.put(buffering.maxBytes());
            output.put(buffering.onFull().code());
        }
    }
}
