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
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodec;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
import com.nebilux.nexalithic.core.io.codec.CodecFrame;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import org.jctools.queues.SpmcArrayQueue;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 业务模型汇编器的类型基类。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class BusinessModelAssembler<M extends BusinessModel<?>> extends BusinessModelCodec implements ModelAssembler<M> {
    private final Map<Integer, CodecContext<M>> assemblingContext = new ConcurrentHashMap<>();
    private final SpmcArrayQueue<M> completedModel;
    private volatile M pendingModel;
    /** 以下状态由所属 Loop 线程修改 */
    private CodecContext<M> blockedContext;
    private Runnable outputAvailableCallback;

    protected BusinessModelAssembler(int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory) {
        super(maxOutstandingContextCount, codecCallbackFactory);
        completedModel = new SpmcArrayQueue<>(maxOutstandingContextCount);
    }

    /** 输出队列恢复容量后，由所属 Loop 重新处理已缓存的输入。 */
    public final void onOutputAvailable(Runnable callback) {
        outputAvailableCallback = Objects.requireNonNull(callback, "outputAvailableCallback");
    }

    @Override
    public boolean feed(LoopBuffer source) throws IOException {
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
        if (blockedContext != null) {
            CodecContext<M> blocked = blockedContext;
            try {
                if (!blocked.flushPending()) {
                    return false;
                }
                blockedContext = null;
            } catch (Throwable failure) {
                blockedContext = null;
                assemblingContext.remove(blocked.modelSequence(), blocked);
                blocked.discard();
                throw failure;
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
            try {
                if (context == null) {
                    if (!frameType.isStart()) {
                        throw new IOException("Invalid model sequence " + modelSequence);
                    }
                    context = onFirstFrame(modelSequence, source, payloadLength);
                    if (context.isComplete()) {
                        context.complete();
                    } else {
                        if (assemblingContext.size() >= maxOutstandingContextCount) {
                            throw new IOException("Too many outstanding business model contexts");
                        }
                        assemblingContext.put(modelSequence, context);
                        onContextRegistered(context);
                    }
                    if (!offerModel(context)) {
                        break;
                    }
                } else {
                    if (!context.nextFrame(frameType, source, payloadLength)) {
                        blockedContext = context;
                        break;
                    }
                    if (context.isComplete()) {
                        context.complete();
                        assemblingContext.remove(modelSequence, context);
                        if (!offerModel(context)) {
                            break;
                        }
                    }
                }
            } catch (Throwable failure) {
                if (context != null) {
                    assemblingContext.remove(modelSequence, context);
                    if (blockedContext == context) {
                        blockedContext = null;
                    }
                    context.discard();
                }
                if (failure instanceof RuntimeException runtime) {
                    throw new IOException("Business frame decoding failed", runtime);
                }
                throw failure;
            }
        }
        return initialReadable != source.readableBytes();
    }

    @Override
    public M drain() {
        M model = completedModel.poll();
        if (model != null) {
            boolean retry;
            synchronized (this) {
                retry = pendingModel != null;
            }
            if (retry && outputAvailableCallback != null) {
                outputAvailableCallback.run();
            }
            return model;
        }
        synchronized (this) {
            model = pendingModel;
            pendingModel = null;
        }
        return model;
    }

    @Override
    public void clear() {
        blockedContext = null;
        for (CodecContext<M> context : assemblingContext.values()) {
            context.discard();
        }
        assemblingContext.clear();
        synchronized (this) {
            pendingModel = null;
        }
        completedModel.clear();
    }

    protected final Runnable outputAvailableCallback() {
        return outputAvailableCallback;
    }

    protected final CodecContext<M> getContext(int modelSequence) {
        return assemblingContext.get(modelSequence);
    }

    protected final boolean removeContext(int modelSequence, CodecContext<M> context) {
        return assemblingContext.remove(modelSequence, context);
    }

    /** 未完成的上下文进入映射后调用。 */
    protected void onContextRegistered(CodecContext<M> context) throws IOException {
    }

    protected CodecContext<M> onFirstFrame(int modelSequence, LoopBuffer input, int payloadLength) throws IOException {
        int surplus = payloadLength - COMMON_BASE_HEADER_SIZE;
        if (surplus < 0) {
            throw new IOException("Business START payload is too short");
        }
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
        CodecContext<M> context = createCodecContext(modelSequence, way, path, correlationId, input, surplus);
        context.firstFrame(payloadLength);
        return context;
    }

    protected abstract CodecContext<M> createCodecContext(int modelSequence, BusinessModel.Way way, short[] path, long correlationId, LoopBuffer input, int surplus) throws IOException;

    private boolean offerModel(CodecContext<M> context) {
        M model = context.model();
        if (model == null || context.delivered) {
            return true;
        }
        context.delivered = true;
        if (completedModel.offer(model)) {
            return true;
        }
        synchronized (this) {
            pendingModel = model;
        }
        return false;
    }

    /**
     * 保存一个业务模型的解码进度；模型创建后即可交付上层。
     *
     * @author Reonvia
     * @since 0.5.0
     */
    public abstract static class CodecContext<M extends BusinessModel<?>> extends Context {
        private final BusinessModelCodecCallback callback;
        private boolean delivered;
        private boolean finished;

        protected CodecContext(int modelSequence, BusinessModelCodecCallback callback) {
            super(modelSequence);
            this.callback = callback;
        }

        protected boolean flushPending() {
            return true;
        }

        protected abstract M model();

        final void firstFrame(int advanced) {
            callback.onStarted(callbackTotalSize());
            callback.onAdvanced(advanced);
        }

        final boolean nextFrame(CodecFrame.Type type, LoopBuffer input, int quota) throws IOException {
            long start = input.getHead();
            boolean accepted = readNextFrame(type, input, quota);
            callback.onAdvanced(input.getHead() - start);
            return accepted;
        }

        /** 未知解码总长度时返回负数。 */
        protected long callbackTotalSize() {
            return -1;
        }

        protected abstract boolean readNextFrame(CodecFrame.Type type, LoopBuffer input, int quota) throws IOException;

        protected abstract boolean isComplete();

        protected void onComplete() {
        }

        protected void onDiscard() {
        }

        private void complete() {
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

        protected final synchronized void discard() {
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
