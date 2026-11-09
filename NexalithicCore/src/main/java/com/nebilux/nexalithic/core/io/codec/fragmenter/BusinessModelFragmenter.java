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
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodec;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
import com.nebilux.nexalithic.core.io.codec.CodecFrame;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
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

    protected BusinessModelFragmenter(int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory) {
        super(maxOutstandingContextCount, codecCallbackFactory);
        incomingContexts = new MpscArrayQueue<>(maxOutstandingContextCount);
    }

    @Override
    public boolean feed(M model) {
        int count;
        do {
            count = outstandingContextCount.get();
            if (count >= maxOutstandingContextCount) {
                return false;
            }
        } while (!outstandingContextCount.compareAndSet(count, count + 1));
        CodecContext<M> context = null;
        try {
            context = createCodecContext(model, nextModelSequence.getAndIncrement());
            if (incomingContexts.offer(context)) {
                signalReadyOutput();
                if (LOGGER_TRACE_ENABLED) {
                    logger.trace("BusinessModelFragmenter feed model [{}]", model);
                }
                return true;
            } else {
                throw new RuntimeException("incomingContexts full");
            }
        } catch (Throwable throwable) {
            if (context != null) {
                discardContext(context);
            }
            throw throwable;
        }
    }

    @Override
    public boolean drain(LoopBuffer output) throws IOException {
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
                    if (!context.isComplete()) {
                        context.nextFrame(output);
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
                    }
                    activeContextCursor = next;
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
    public boolean isEmpty() {
        return outstandingContextCount.get() == 0;
    }

    @Override
    public boolean isReady() {
        return hasReadyContext.get();
    }

    /** 调用方须保证清理时不再有线程提交或排空上下文。 */
    @Override
    public void clear() {
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
            logger.trace("BusinessModelFragmenter drain model [{}]", context.model());
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
        private boolean finished;

        protected CodecContext(int modelSequence, BusinessModelCodecCallback callback, M model) {
            super(modelSequence);
            this.callback = callback;
            this.model = model;
        }

        protected final M model() {
            return model;
        }

        /** 按模型头大小预留空间，写入 START 载荷后回填帧头。 */
        final boolean firstFrame(LoopBuffer output) throws IOException {
            int headerSize = headerSize();
            if (headerSize <= 0 || headerSize > CodecFrame.MAX_PAYLOAD_LENGTH) {
                throw new IOException("Invalid START header size: " + headerSize);
            }
            int required = CodecFrame.HEADER_LENGTH + headerSize;
            if (output.writableBytes() < required) {
                return false;
            }
            callback.onStarted(callbackTotalSize());
            long start = output.getTail();
            try {
                output.advanceTail(CodecFrame.HEADER_LENGTH);
                writeFirst(output);
                long end = output.getTail();
                if (end - start != required) {
                    throw new IOException("START frame payload length mismatch: " + headerSize);
                }
                output.setTail(start);
                output.put(CodecFrame.packStart(modelSequence(), headerSize));
                output.setTail(end);
                onFirstFrameCommitted(headerSize);
                callback.onAdvanced(headerSize);
                return true;
            } catch (Throwable failure) {
                output.setTail(start);
                throw failure;
            }
        }

        /** 写入 DATA 载荷后回填帧头；终止帧由具体上下文处理。 */
        final boolean nextFrame(LoopBuffer output) throws IOException {
            if (isComplete() || output.writableBytes() <= CodecFrame.HEADER_LENGTH * MIN_HEADER_COUNT) {
                return false;
            }
            long start = output.getTail();
            try {
                output.advanceTail(CodecFrame.HEADER_LENGTH);
                writeNextFrame(output, Math.min(output.writableBytes(), CodecFrame.MAX_PAYLOAD_LENGTH));
                long end = output.getTail();
                int consumed = (int) (end - start - CodecFrame.HEADER_LENGTH);
                if (consumed == 0) {
                    output.setTail(start);
                    return tryWriteLastFrame(output);
                }
                output.setTail(start);
                output.put(CodecFrame.packData(modelSequence(), consumed));
                output.setTail(end);
                onNextFrameCommitted(consumed);
                callback.onAdvanced(consumed);
            } catch (Throwable failure) {
                output.setTail(start);
                throw failure;
            }
            tryWriteLastFrame(output);
            return true;
        }

        /** 未知解码总长度时返回负数。 */
        protected long callbackTotalSize() {
            return -1;
        }

        /** 子类模型头在共有字段之外占用的字节数。 */
        protected abstract int extraHeaderSize();

        /** 写入路径之后的模型专属字段。 */
        protected abstract void writeExtraFirstFrame(LoopBuffer output) throws IOException;

        /** 帧头与载荷均写入成功后调用。 */
        protected void onFirstFrameCommitted(int advanced) {
        }

        /** 最多写入 {@code quota} 字节的 DATA 载荷；实际长度由缓冲区写入位置确定。 */
        protected abstract void writeNextFrame(LoopBuffer output, int quota) throws IOException;

        /** DATA 帧提交后更新模型自身的编码进度。 */
        protected void onNextFrameCommitted(int advanced) {
        }

        /** DATA 后或暂无 DATA 时尝试写入模型终止帧。 */
        protected boolean tryWriteLastFrame(LoopBuffer output) throws IOException {
            return false;
        }

        /** 不执行完成回调的状态查询。 */
        protected abstract boolean isComplete();

        protected abstract boolean isReady();

        protected void onComplete() {}

        protected void onDiscard() {}

        protected int headerSize() {
            return COMMON_BASE_HEADER_SIZE + model().getPathDepth() * Short.BYTES + extraHeaderSize();
        }

        /** 写入共有模型头；子类可在指定位置补充自己的字段。 */
        private void writeFirst(LoopBuffer output) throws IOException {
            output.put(model().getCorrelationId());
            output.put(model().getWay().code());
            byte pathDepth = model().getPathDepth();
            output.put(pathDepth);
            if (pathDepth > 0) {
                for (short value : model().getPath()) {
                    output.put(value);
                }
            }
            writeExtraFirstFrame(output);
        }

        /** 成功完成时调用一次。 */
        private void complete() {
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
        private void discard() {
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
    }
}
