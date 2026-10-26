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

package com.nebilux.nexalithic.core.internal.model.stream;

import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.StreamDrainer;
import com.nebilux.nexalithic.core.model.business.stream.StreamFeeder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 业务流的队列实现。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class BusinessStreamImpl<C extends BusinessStream.Chunk> extends BusinessStream<C> implements StreamCodecAccess<C> {
    private static final Logger logger = LoggerFactory.getLogger(BusinessStreamImpl.class);

    private final ChunkQueue<C> input;
    private final ChunkQueue<C> output;
    private final StreamFeeder<C> feeder;
    private final StreamDrainer<C> drainer;

    public BusinessStreamImpl(Class<C> type, Direction direction, Policy policy, BusinessModel.Way way, short... path) {
        super(type, direction, policy, way, path);
        input = direction.hasInput() ? new ChunkQueue<>(policy.sequence(), policy.input(), this::onCallbackFailure) : null;
        output = direction.hasOutput() ? new ChunkQueue<>(policy.sequence(), policy.output(), this::onCallbackFailure) : null;
        feeder = input == null ? null : new StreamFeeder<>() {
            @Override
            public OfferResult offerChunk(C chunk) {
                return input.offer(chunk);
            }

            @Override
            public void finish() {
                input.finish();
            }

            @Override
            public void onAvailable(Runnable callback) {
                input.onWritable(callback);
            }
        };
        drainer = output == null ? null : new StreamDrainer<>() {
            @Override
            public C pollChunk() {
                return output.poll();
            }

            @Override
            public boolean isFinished() {
                return output.isFinished();
            }

            @Override
            public void onAvailable(Runnable callback) {
                output.onReadable(callback);
            }
        };
    }

    @Override
    public StreamFeeder<C> feeder() {
        return feeder;
    }

    @Override
    public StreamDrainer<C> drainer() {
        return drainer;
    }

    @Override
    public C pollInputChunk() {
        return requireInput().poll();
    }

    @Override
    public boolean isInputFinished() {
        return requireInput().isFinished();
    }

    @Override
    public void onInputAvailable(Runnable callback) {
        requireInput().onReadable(callback);
    }

    @Override
    public StreamFeeder.OfferResult offerOutputChunk(C chunk) {
        return requireOutput().offer(chunk);
    }

    @Override
    public void finishOutput() {
        requireOutput().finish();
    }

    @Override
    public void onOutputAvailable(Runnable callback) {
        requireOutput().onWritable(callback);
    }

    private ChunkQueue<C> requireInput() {
        if (input == null) {
            throw new IllegalStateException("Stream has no input direction");
        }
        return input;
    }

    private ChunkQueue<C> requireOutput() {
        if (output == null) {
            throw new IllegalStateException("Stream has no output direction");
        }
        return output;
    }

    private void onCallbackFailure(RuntimeException failure) {
        recordCallbackFailure(failure);
        logger.warn("Stream availability callback failed", failure);
    }

    private static final class ChunkQueue<C extends BusinessStream.Chunk> {
        private final BusinessStream.Policy.Sequence sequence;
        private final BusinessStream.Policy.Buffering buffering;
        private final Consumer<RuntimeException> callbackFailureHandler;
        private final ArrayDeque<C> chunks = new ArrayDeque<>();
        private Runnable readableCallback;
        private Runnable writableCallback;
        private long queuedBytes;
        private long lastSequence = -1;
        private boolean finished;
        private int retryBytes = -1;

        private ChunkQueue(BusinessStream.Policy.Sequence sequence, BusinessStream.Policy.Buffering buffering, Consumer<RuntimeException> callbackFailureHandler) {
            this.sequence = sequence;
            this.buffering = buffering;
            this.callbackFailureHandler = callbackFailureHandler;
        }

        private StreamFeeder.OfferResult offer(C chunk) {
            Objects.requireNonNull(chunk, "chunk");
            int bytes = chunk.size();
            if (bytes < 0 || bytes > buffering.maxBytes()) {
                throw new IllegalArgumentException("Chunk size exceeds the queue byte limit");
            }
            Runnable callback = null;
            synchronized (this) {
                if (finished) {
                    throw new IllegalStateException("Stream direction is finished");
                }
                validateSequence(chunk.sequence());
                if (!hasCapacity(bytes)) {
                    switch (buffering.onFull()) {
                        case BACKPRESSURE -> {
                            retryBytes = retryBytes < 0 ? bytes : Math.min(retryBytes, bytes);
                            return StreamFeeder.OfferResult.RETRY;
                        }
                        case DROP_INCOMING -> {
                            lastSequence = chunk.sequence();
                            return StreamFeeder.OfferResult.DROPPED;
                        }
                        case DROP_HEAD, DROP_TAIL -> {
                            while (!hasCapacity(bytes)) {
                                C removed = buffering.onFull() == BusinessStream.Policy.OnFull.DROP_HEAD
                                        ? chunks.removeFirst() : chunks.removeLast();
                                queuedBytes -= removed.size();
                            }
                        }
                    }
                }
                boolean wasEmpty = chunks.isEmpty();
                chunks.addLast(chunk);
                queuedBytes += bytes;
                lastSequence = chunk.sequence();
                if (wasEmpty) {
                    callback = readableCallback;
                }
            }
            notifyCallback(callback);
            return StreamFeeder.OfferResult.ENQUEUED;
        }

        private C poll() {
            C chunk;
            Runnable callback = null;
            synchronized (this) {
                boolean hadCapacity = hasCapacity(0);
                chunk = chunks.pollFirst();
                if (chunk == null) {
                    return null;
                }
                queuedBytes -= chunk.size();
                if (!finished && writableCallback != null
                        && (retryBytes >= 0 ? hasCapacity(retryBytes) : !hadCapacity && hasCapacity(0))) {
                    callback = writableCallback;
                    retryBytes = -1;
                }
            }
            notifyCallback(callback);
            return chunk;
        }

        private void finish() {
            Runnable callback;
            synchronized (this) {
                if (finished) {
                    return;
                }
                finished = true;
                callback = readableCallback;
            }
            notifyCallback(callback);
        }

        private synchronized boolean isFinished() {
            return finished && chunks.isEmpty();
        }

        private void onReadable(Runnable callback) {
            boolean notify;
            synchronized (this) {
                readableCallback = callback;
                notify = callback != null && (!chunks.isEmpty() || finished);
            }
            if (notify) {
                notifyCallback(callback);
            }
        }

        private void onWritable(Runnable callback) {
            boolean notify;
            synchronized (this) {
                writableCallback = callback;
                notify = callback != null && !finished && hasCapacity(Math.max(retryBytes, 0));
                if (notify) {
                    retryBytes = -1;
                }
            }
            if (notify) {
                notifyCallback(callback);
            }
        }

        private void notifyCallback(Runnable callback) {
            if (callback == null) {
                return;
            }
            try {
                callback.run();
            } catch (RuntimeException failure) {
                callbackFailureHandler.accept(failure);
            }
        }

        private boolean hasCapacity(int bytes) {
            return chunks.size() < buffering.maxChunks() && bytes <= buffering.maxBytes() - queuedBytes;
        }

        private void validateSequence(long next) {
            if (lastSequence < 0) {
                return;
            }
            if (sequence == BusinessStream.Policy.Sequence.CONTIGUOUS) {
                if (lastSequence == Long.MAX_VALUE || next != lastSequence + 1) {
                    throw new IllegalArgumentException("Chunk sequence must be contiguous");
                }
            } else if (next <= lastSequence) {
                throw new IllegalArgumentException("Chunk sequence must increase");
            }
        }
    }
}
