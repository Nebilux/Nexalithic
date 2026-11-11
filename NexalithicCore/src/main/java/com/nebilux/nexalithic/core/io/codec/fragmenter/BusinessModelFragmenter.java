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

import com.nebilux.nexalithic.core.connection.session.SessionKey;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodec;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
import com.nebilux.nexalithic.core.io.codec.CodecFrame;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.codec.PayloadCodec;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务模型碎片器的类型基类。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class BusinessModelFragmenter<M extends BusinessModel<?>> extends BusinessModelCodec implements ModelFragmenter<M> {
    private static final Logger logger = LoggerFactory.getLogger(BusinessModelFragmenter.class);
    private static final boolean LOGGER_TRACE_ENABLED = logger.isTraceEnabled();

    /** 外部线程提交的上下文；由所属 Loop 线程取出。 */
    private final MpscArrayQueue<CodecContext<M>> incomingContexts;
    /** 每个通道独立分配，仅用于关联该模型的编解码帧。 */
    private final AtomicInteger nextModelSequence = new AtomicInteger();
    private final AtomicBoolean hasReadyContext = new AtomicBoolean();
    /** 以下字段仅由所属 Loop 线程访问。 */
    private CodecContext<M> pendingContext;
    private CodecContext<M> activeContextCursor;
    private int activeContextCount;

    protected BusinessModelFragmenter(SessionKey sessionKey, int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory) {
        super(sessionKey, maxOutstandingContextCount, codecCallbackFactory);
        incomingContexts = new MpscArrayQueue<>(maxOutstandingContextCount);
    }

    @Override
    public final boolean feed(M model) {
        int count;
        do {
            count = outstandingContextCount.get();
            if (count >= maxOutstandingContextCount) {
                return false;
            }
        } while (!outstandingContextCount.compareAndSet(count, count + 1));
        CodecContext<M> context = null;
        try {
            int modelSequence = nextModelSequence.incrementAndGet();
            context = createCodecContext(model, modelSequence);
            if (incomingContexts.offer(context)) {
                signalReadyOutput();
                if (LOGGER_TRACE_ENABLED) {
                    logger.trace("accepted [{}] - [modelSequence: {}, correlationId: {}]",
                            sessionKey, modelSequence, model.getCorrelationId());
                }
                return true;
            } else {
                throw new RuntimeException("incomingContexts full");
            }
        } catch (Throwable throwable) {
            outstandingContextCount.decrementAndGet();
            if (context != null) {
                discardContext(context);
            }
            throw throwable;
        }
    }

    @Override
    public final boolean drain(LoopBuffer output) throws IOException {
        int initialWritable = output.writableBytes();
        draining: {
            boolean hasReadyContext = false;
            CodecContext<M> context = null;
            try {
                for (context = takeInitialContext(); context != null; context = incomingContexts.poll()) {
                    if (!context.firstFrame(output)) {
                        pendingContext = context;
                        if (!hasReadyContext) {
                            hasReadyContext = context.isReady();
                        }
                        break draining;
                    }
                    if (context.isComplete()) {
                        completeContext(context);
                    } else {
                        appendContextToActiveCursor(context);
                        if (!hasReadyContext) {
                            hasReadyContext = context.isReady();
                        }
                    }
                }
                for (int stalled = 0; (context = activeContextCursor) != null;) {
                    CodecContext<M> next = context.next;
                    if (!context.nextFrame(output)) {
                        boolean ready = context.isReady();
                        if (!hasReadyContext) {
                            hasReadyContext = ready;
                        }
                        if (ready || ++stalled >= activeContextCount) {
                            break draining;
                        }
                    } else {
                        stalled = 0;
                    }
                    if (context.isComplete()) {
                        completeContext(context);
                    } else {
                        if (!hasReadyContext) {
                            hasReadyContext = context.isReady();
                        }
                        activeContextCursor = next;
                    }
                }
            } catch (Throwable throwable) {
                if (context != null) {
                    discardContext(context);
                }
                throw throwable;
            } finally {
                this.hasReadyContext.set(hasReadyContext);
            }
        }
        return initialWritable != output.writableBytes();
    }

    @Override
    public final boolean isEmpty() {
        return outstandingContextCount.get() == 0;
    }

    @Override
    public final boolean isReady() {
        return hasReadyContext.get();
    }

    /** 调用方须保证清理时不再有线程提交或排空上下文。 */
    @Override
    public final void clear() {
        hasReadyContext.set(false);
        for (CodecContext<M> context = takeInitialContext(); context != null; context = incomingContexts.poll()) {
            discardContext(context);
        }
        while (activeContextCursor != null) {
            discardContext(activeContextCursor);
        }
    }

    /** 新输入到达时由生产线程通知写端；调用方还需唤醒所属通道。 */
    protected final void signalReadyOutput() {
        hasReadyContext.set(true);
    }

    protected abstract CodecContext<M> createCodecContext(M model, int modelSequence);

    private CodecContext<M> takeInitialContext() {
        CodecContext<M> result = pendingContext;
        pendingContext = null;
        return result != null ? result : incomingContexts.poll();
    }

    private void completeContext(CodecContext<M> context) {
        retireContext(context);
        context.complete();
        if (LOGGER_TRACE_ENABLED) {
            M model = context.model();
            logger.trace("completed [{}] - [modelSequence={}, correlationId={}, model: [{}]]",
                    sessionKey, context.modelSequence(), model.getCorrelationId(), model);
        }
    }

    private void discardContext(CodecContext<M> context) {
        retireContext(context);
        context.discard();
    }

    private void retireContext(CodecContext<M> context) {
        if (context.finished) {
            return;
        }
        if (context.prev != null) {
            removeContextFromActiveCursor(context);
        }
        outstandingContextCount.decrementAndGet();
    }

    private void appendContextToActiveCursor(CodecContext<M> context) {
        if (activeContextCursor == null) {
            context.prev = context;
            context.next = context;
            activeContextCursor = context;
        } else {
            CodecContext<M> tail = activeContextCursor.prev;
            tail.next = context;
            context.prev = tail;
            context.next = activeContextCursor;
            activeContextCursor.prev = context;
        }
        activeContextCount++;
    }

    private void removeContextFromActiveCursor(CodecContext<M> context) {
        if (context.next == context) {
            activeContextCursor = null;
        } else {
            context.prev.next = context.next;
            context.next.prev = context.prev;
            if (activeContextCursor == context) {
                activeContextCursor = context.next;
            }
        }
        context.prev = null;
        context.next = null;
        activeContextCount--;
    }

    /** 保存一个业务模型的分帧进度及其在活跃环形链表中的位置。 */
    protected abstract static class CodecContext<M extends BusinessModel<?>> extends Context {
        private final BusinessModelCodecCallback callback;
        private final M model;
        /** 未加入活跃链表时均为 null；仅由所属 Loop 线程修改。 */
        private CodecContext<M> prev;
        private CodecContext<M> next;
        private boolean complete;
        private boolean finished;

        protected CodecContext(int modelSequence, BusinessModelCodecCallback callback, M model) {
            super(modelSequence);
            this.callback = callback;
            this.model = model;
        }

        protected final M model() {
            return model;
        }

        final boolean firstFrame(LoopBuffer output) throws IOException {
            int headerSize = headerSize();
            int surplus = output.writableBytes() - CodecFrame.HEADER_LENGTH - headerSize;
            if (surplus < 0) {
                return false;
            }
            callback.onStarted(encodedTotalSize());
            long advanceTailBefore = output.getTail();
            output.advanceTail(CodecFrame.HEADER_LENGTH);
            try {
                long writeHeaderBefore = output.getTail();
                writeHeader(output);
                writePayload(output, Math.min(surplus, CodecFrame.MAX_PAYLOAD_LENGTH - headerSize));
                long writePayloadAfter = output.getTail();
                output.setTail(advanceTailBefore);
                int consumed = (int) (writePayloadAfter - writeHeaderBefore);
                boolean hasPayload = hasPayload();
                CodecFrame.Type frameType = hasPayload ? CodecFrame.Type.START : CodecFrame.Type.SINGLE;
                output.unsafePut(CodecFrame.pack(frameType, modelSequence(), consumed));
                output.setTail(writePayloadAfter);
                onFirstFrameCommitted(frameType, consumed);
                callback.onAdvanced(consumed);
                complete = !hasPayload;
            } catch (Throwable throwable) {
                output.setTail(advanceTailBefore);
                throw throwable;
            }
            return true;
        }

        final boolean nextFrame(LoopBuffer output) throws IOException {
            int surplus = output.writableBytes() - CodecFrame.HEADER_LENGTH;
            if (surplus < 0) {
                return false;
            }
            long advanceTailBefore = output.getTail();
            output.advanceTail(CodecFrame.HEADER_LENGTH);
            try {
                long writePayloadBefore = output.getTail();
                writePayload(output, Math.min(surplus, CodecFrame.MAX_PAYLOAD_LENGTH));
                long writePayloadAfter = output.getTail();
                output.setTail(advanceTailBefore);
                int consumed = (int) (writePayloadAfter - writePayloadBefore);
                boolean hasPayload = hasPayload();
                if (consumed == 0 && hasPayload) {
                    return false;
                }
                CodecFrame.Type frameType = hasPayload ? CodecFrame.Type.DATA : CodecFrame.Type.END;
                output.unsafePut(CodecFrame.pack(frameType, modelSequence(), consumed));
                output.setTail(writePayloadAfter);
                onNextFrameCommitted(frameType, consumed);
                callback.onAdvanced(consumed);
                complete = !hasPayload;
            } catch (Throwable failure) {
                output.setTail(advanceTailBefore);
                throw failure;
            }
            return true;
        }

        /** 成功完成时调用一次。 */
        final void complete() {
            if (finished) {
                return;
            }
            finished = true;
            try {
                onComplete();
            } finally {
                callback.onCompleted(modelSequence(), model.getCorrelationId(), model.getWay());
            }
        }

        /** 编码失败或清理时调用一次。 */
        final void discard() {
            if (finished) {
                return;
            }
            finished = true;
            try {
                onDiscard();
            } finally {
                callback.onDiscarded();
            }
        }

        final boolean isComplete() {
            return complete;
        }

        abstract boolean isReady();

        /** 未知总长度时返回负数。 */
        protected long encodedTotalSize() {
            return -1;
        }

        /** 子类模型头在共有字段之外占用的字节数。 */
        protected abstract int extraHeaderSize();

        /** 写入路径之后的模型专属字段。 */
        protected abstract void writeExtraHeader(LoopBuffer output) throws IOException;

        protected abstract void writePayload(LoopBuffer output, int quota) throws IOException;

        protected abstract boolean hasPayload();

        protected void onFirstFrameCommitted(CodecFrame.Type farmeType, int payloadLength) {}

        protected void onNextFrameCommitted(CodecFrame.Type farmeType, int payloadLength) {}

        protected void onComplete() {}

        protected void onDiscard() {}

        protected final int headerSize() {
            return COMMON_BASE_HEADER_SIZE + model().getPathDepth() * Short.BYTES + extraHeaderSize();
        }

        protected static int encode(PayloadCodec.Encoder encoder, long remaining, LoopBuffer output, int quota) throws IOException {
            quota = (int) Math.min(remaining, quota);
            LoopBuffer.LimitedWritableView view = output.unsafeLimitedWritableView(quota);
            encoder.encode(view);
            return quota - view.remaining();
        }

        /** 写入共有模型头；子类可在指定位置补充自己的字段。 */
        private void writeHeader(LoopBuffer output) throws IOException {
            output.unsafePut(model().getCorrelationId());
            output.unsafePut(model().getWay().code());
            byte pathDepth = model().getPathDepth();
            output.unsafePut(pathDepth);
            if (pathDepth > 0) {
                for (short value : model().getPath()) {
                    output.unsafePut(value);
                }
            }
            writeExtraHeader(output);
        }
    }
}
