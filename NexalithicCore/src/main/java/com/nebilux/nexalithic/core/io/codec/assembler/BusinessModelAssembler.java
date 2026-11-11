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

import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.connection.session.SessionKey;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.concurrent.atomic.AtomicAdmissionQueue;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.infra.timer.TimerContext;
import com.nebilux.nexalithic.core.infra.timer.TimerCoordinator;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodec;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
import com.nebilux.nexalithic.core.io.codec.CodecFrame;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.codec.PayloadCodec;
import org.jctools.queues.SpmcArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 业务模型汇编器的类型基类。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class BusinessModelAssembler<M extends BusinessModel<?>> extends BusinessModelCodec implements ModelAssembler<M>, TimerCoordinator<BusinessModelAssembler.CodecContext<?>> {
    protected static abstract class Options extends BusinessModelCodec.Options {
        public final NexalithicOption<Long> MaxIdleTimeMillis = defineOption(
                30_000L, OptionValidator.positive()
        );

        protected Options(Class<?> holder) {
            super(holder);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(BusinessModelAssembler.class);
    private static final boolean LOGGER_TRACE_ENABLED = logger.isTraceEnabled();

    private final Map<Integer, CodecContext<M>> assemblingContext = new ConcurrentHashMap<>();
    private final SpmcArrayQueue<M> completedModel;
    private final TimeWheel<CodecContext<?>> timeWheel;
    private final long maxIdleTimeNanos;
    private final AtomicAdmissionQueue<CodecContext<?>> expiredContexts = new AtomicAdmissionQueue<>(new ConcurrentLinkedQueue<>());
    private volatile M pendingModel;
    /** 以下状态由所属 Loop 线程修改 */
    private Runnable outputAvailableCallback;
    private Runnable timeoutAvailableCallback;

    protected BusinessModelAssembler(SessionKey sessionKey, int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory,
                                     long maxIdleTimeNanos, TimeWheel<CodecContext<?>> timeWheel) {
        super(sessionKey, maxOutstandingContextCount, codecCallbackFactory);
        if (maxIdleTimeNanos <= 0) {
            throw new IllegalArgumentException("maxIdleTimeNanos must be positive");
        }
        completedModel = new SpmcArrayQueue<>(maxOutstandingContextCount);
        this.maxIdleTimeNanos = maxIdleTimeNanos;
        this.timeWheel = timeWheel;
    }

    /** 输出队列恢复容量后，由所属 Loop 重新处理已缓存的输入。 */
    public final void onOutputAvailable(Runnable callback) {
        outputAvailableCallback = callback;
    }

    /** 到期上下文入队后唤醒所属 Loop。 */
    public final void onTimeoutAvailable(Runnable callback) {
        timeoutAvailableCallback = callback;
    }

    /** 仅由所属 Loop 调用；处理时间轮投递的上下文。 */
    public final void processExpiredContexts() {
        for (CodecContext<?> context; (context = expiredContexts.poll()) != null;) {
            try {
                if (assemblingContext.get(context.modelSequence()) != context || context.finished || context.isComplete()) {
                    continue;
                }
                if (System.nanoTime() < context.lastActiveTimeNanos + maxIdleTimeNanos) {
                    try {
                        timeWheel.schedule(context, this);
                    } catch (Throwable failure) {
                        assemblingContext.remove(context.modelSequence(), context);
                        try {
                            context.discard();
                        } catch (Throwable discardFailure) {
                            failure.addSuppressed(discardFailure);
                        }
                        throw failure;
                    }
                    continue;
                }
                if (assemblingContext.remove(context.modelSequence(), context)) {
                    context.discard();
                }
            } catch (Throwable throwable) {
                logger.error("Assembly timeout processing failed, modelSequence={}", context.modelSequence(), throwable);
            }
        }
    }

    @Override
    public final boolean feed(LoopBuffer source) throws IOException {
        int initialReadable = source.readableBytes();
        if (pendingModel != null) {
            synchronized (this) {
                if (pendingModel != null) {
                    if (!completedModel.offer(pendingModel)) {
                        return false;
                    }
                    pendingModel = null;
                }
            }
        }
        while (source.readableBytes() >= CodecFrame.HEADER_LENGTH) {
            source.markHead();
            long header = source.unsafeGetLong();
            CodecFrame.Type frameType;
            try {
                frameType = CodecFrame.parseFrameType(header);
            } catch (IllegalArgumentException failure) {
                throw new IOException("Invalid business frame type", failure);
            }
            int payloadLength = CodecFrame.parsePayloadLength(header);
            int modelSequence = CodecFrame.parseModelSequence(header);
            if (payloadLength > CodecFrame.MAX_PAYLOAD_LENGTH) {
                throw new IOException("Business frame payload exceeds maximum: " + payloadLength);
            }
            if (source.readableBytes() < payloadLength) {
                source.resetHead();
                break;
            }
            CodecContext<M> context = assemblingContext.get(modelSequence);
            long readBefore = source.getHead();
            try {
                boolean firstFrame = frameType.isSingle() || frameType.isStart();
                if (firstFrame) {
                    if (context != null) {
                        throw new IOException("Invalid model sequence " + modelSequence);
                    }
                    context = onFirstFrame(modelSequence, source, payloadLength, frameType);
                } else {
                    if (context == null) {
                        throw new IOException("Invalid model sequence " + modelSequence);
                    }
                    context.nextFrame(source, payloadLength, frameType);
                }
                long readAfter = source.getHead();
                int surplus = payloadLength - (int) (readAfter - readBefore);
                boolean complete = context.isComplete();
                boolean retryFrame = surplus > 0 || ((frameType.isSingle() || frameType.isEnd()) && !complete);
                if (retryFrame) {
                    source.markTail();
                    source.setTail(readAfter - CodecFrame.HEADER_LENGTH);
                    source.unsafePut(CodecFrame.pack(switch (frameType) {
                        case SINGLE, END -> CodecFrame.Type.END;
                        case START, DATA -> CodecFrame.Type.DATA;
                    }, modelSequence, surplus));
                    source.resetTail();
                    source.advanceHead(-CodecFrame.HEADER_LENGTH);
                }
                if (firstFrame && !complete) {
                    if (assemblingContext.size() >= maxOutstandingContextCount) {
                        throw new IOException("Too many outstanding business model contexts");
                    }
                    assemblingContext.put(modelSequence, context);
                    timeWheel.schedule(context, this);
                } else if (complete && !firstFrame) {
                    assemblingContext.remove(modelSequence, context);
                }
                if (complete) {
                    context.complete();
                    if (LOGGER_TRACE_ENABLED) {
                        M model = context.model();
                        logger.trace("completed [{}] - [modelSequence={}, correlationId={}, model: [{}]]",
                                sessionKey, modelSequence, model.getCorrelationId(), model);
                    }
                }
                boolean canContinue = !(complete || (firstFrame && deliverOnFirstFrame())) || deliveredModel(context);
                if (retryFrame || !canContinue) {
                    break;
                }
            } catch (Throwable failure) {
                source.setHead(readBefore + payloadLength);
                if (context != null) {
                    assemblingContext.remove(modelSequence, context);
                    context.discard();
                }
                throw failure;
            }
        }
        return initialReadable != source.readableBytes();
    }

    @Override
    public final M drain() {
        M model = completedModel.poll();
        boolean retry;
        synchronized (this) {
            if (model == null) {
                model = pendingModel;
                pendingModel = null;
                retry = model != null;
            } else {
                retry = pendingModel != null;
            }
        }
        if (retry && outputAvailableCallback != null) {
            outputAvailableCallback.run();
        }
        return model;
    }

    @Override
    public final void clear() {
        expiredContexts.close();
        expiredContexts.drain(context -> {});
        for (CodecContext<M> context : assemblingContext.values()) {
            context.discard();
        }
        assemblingContext.clear();
        synchronized (this) {
            pendingModel = null;
        }
        completedModel.clear();
    }

    @Override
    public final long getExpiryTimeNanos(TimerContext<CodecContext<?>> timer) {
        return timer.target().lastActiveTimeNanos + maxIdleTimeNanos;
    }

    @Override
    public final boolean isCancelled(TimerContext<CodecContext<?>> timer) {
        CodecContext<?> context = timer.target();
        return context.finished || context.isComplete()
                || assemblingContext.get(context.modelSequence()) != context;
    }

    @Override
    public final boolean onExpiryTrigger(TimerContext<CodecContext<?>> timer) {
        CodecContext<?> context = timer.target();
        if (isCancelled(timer)) {
            return true;
        }
        if (System.nanoTime() < context.lastActiveTimeNanos + maxIdleTimeNanos) {
            return false;
        }
        if (!expiredContexts.offer(context)) {
            return true;
        }
        timeoutAvailableCallback.run();
        return true;
    }

    protected final Runnable outputAvailableCallback() {
        return outputAvailableCallback;
    }

    /** 模型尚未解码完成时，是否在首帧后交付给上层。 */
    protected abstract boolean deliverOnFirstFrame();

    protected abstract CodecContext<M> createCodecContext(int modelSequence, BusinessModel.Way way, short[] path, long correlationId, LoopBuffer input, int surplus) throws IOException;

    private CodecContext<M> onFirstFrame(int modelSequence, LoopBuffer input, int surplus, CodecFrame.Type frameType) throws IOException {
        if ((surplus -= COMMON_BASE_HEADER_SIZE) < 0) {
            throw new IOException("Business START payload is too short");
        }
        long readHeaderBefore = input.getHead();
        long correlationId = input.unsafeGetLong();
        BusinessModel.Way way = BusinessModel.Way.from(input.unsafeGetShort());
        byte pathDepth = input.unsafeGetByte();
        if (pathDepth > BusinessModel.MAX_PATH_DEPTH) {
            throw new IOException("Invalid business path depth: " + pathDepth);
        }
        if ((surplus -= pathDepth * Short.BYTES) < 0) {
            throw new IOException("Business START path exceeds payload length");
        }
        short[] path = new short[pathDepth];
        for (int i = 0; i < pathDepth; i++) {
            path[i] = input.unsafeGetShort();
        }
        long readExtraHeaderBefore = input.getHead();
        CodecContext<M> context = createCodecContext(modelSequence, way, path, correlationId, input, surplus);
        long readExtraHeaderAfter = input.getHead();
        context.firstFrame(input, surplus - (int) (readExtraHeaderAfter - readExtraHeaderBefore), frameType, (int) (readExtraHeaderAfter - readHeaderBefore));
        if (LOGGER_TRACE_ENABLED) {
            logger.trace("accepted [{}] - [modelSequence: {}, correlationId: {}]",
                    sessionKey, modelSequence, correlationId);
        }
        return context;
    }

    private boolean deliveredModel(CodecContext<M> context) {
        if (context.delivered) {
            return true;
        }
        M model = Objects.requireNonNull(context.model(), "deliverable model");
        context.delivered = true;
        boolean queued = completedModel.offer(model);
        if (!queued) {
            synchronized (this) {
                pendingModel = model;
            }
        }
        return queued;
    }

    /**
     * 保存一个业务模型的解码进度；模型创建后即可交付上层。
     *
     * @author Reonvia
     * @since 0.5.0
     */
    public abstract static class CodecContext<M extends BusinessModel<?>> extends Context {
        private final BusinessModelCodecCallback callback;
        private volatile boolean complete;
        private volatile boolean finished;
        private boolean delivered;
        private volatile long lastActiveTimeNanos = System.nanoTime();

        protected CodecContext(int modelSequence, BusinessModelCodecCallback callback) {
            super(modelSequence);
            this.callback = callback;
        }

        final void firstFrame(LoopBuffer input, int quota, CodecFrame.Type frameType, int headerSize) throws IOException {
            callback.onStarted(callbackTotalSize());
            int consumed = decodePayload(input, quota);
            lastActiveTimeNanos = System.nanoTime();
            callback.onAdvanced(consumed + headerSize);
            if (frameType.isSingle() && consumed == quota) {
                complete = tryCompleteDecoding();
            }
        }

        final synchronized void nextFrame(LoopBuffer input, int quota, CodecFrame.Type frameType) throws IOException {
            if (finished) {
                throw new IOException("Business model context is no longer active");
            }
            int consumed = decodePayload(input, quota);
            if (consumed > 0) {
                lastActiveTimeNanos = System.nanoTime();
            }
            callback.onAdvanced(consumed);
            if (frameType.isEnd() && consumed == quota) {
                complete = tryCompleteDecoding();
            }
        }

        abstract M model();

        final void complete() {
            if (finished) {
                return;
            }
            finished = true;
            try {
                onComplete();
            } finally {
                M model = model();
                callback.onCompleted(modelSequence(), model.getCorrelationId(), model.getWay());
            }
        }

        final synchronized void discard() {
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

        /** 未知解码总长度时返回负数。 */
        protected long callbackTotalSize() {
            return -1;
        }

        /** 解码当前帧中的模型载荷；调用方会校验是否恰好消耗 {@code quota} 字节。 */
        protected abstract void readPayload(LoopBuffer input, int quota) throws IOException;

        /** SINGLE 或 END 的载荷解码完毕后校验并结束模型。 */
        protected abstract boolean tryCompleteDecoding() throws IOException;

        protected void onComplete() {
        }

        protected void onDiscard() {
        }

        protected static int decode(PayloadCodec.Decoder<?> decoder, long remaining, LoopBuffer input, int quota) throws IOException {
            quota = (int) Math.min(remaining, quota);
            LoopBuffer.LimitedReadableView view = input.unsafeLimitedReadableView(quota);
            decoder.decode(view);
            return quota - view.remaining();
        }

        private int decodePayload(LoopBuffer input, int quota) throws IOException {
            long readPayloadBefore = input.getHead();
            readPayload(input, quota);
            return (int) (input.getHead() - readPayloadBefore);
        }
    }
}
