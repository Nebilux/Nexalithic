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

package com.nebilux.nexalithic.core.io.codec;

import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.io.codec.assembler.BusinessModelAssembler;
import com.nebilux.nexalithic.core.io.codec.assembler.BusinessPacketAssembler;
import com.nebilux.nexalithic.core.io.codec.assembler.BusinessStreamAssembler;
import com.nebilux.nexalithic.core.io.codec.assembler.SignalingPacketAssembler;
import com.nebilux.nexalithic.core.io.codec.fragmenter.BusinessModelFragmenter;
import com.nebilux.nexalithic.core.io.codec.fragmenter.BusinessPacketFragmenter;
import com.nebilux.nexalithic.core.io.codec.fragmenter.BusinessStreamFragmenter;
import com.nebilux.nexalithic.core.io.codec.fragmenter.SignalingPacketFragmenter;
import com.nebilux.nexalithic.core.messaging.task.NexalithicTask;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferListener;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodecRegistry;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodecRegistry;

import java.util.concurrent.TimeUnit;

/**
 * 编解码器供应商
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class CodecSupplier {
    private final int signalingPacketFragmenter_maxOutstandingContextCount;
    private final int businessPacketFragmenter_maxOutstandingContextCount;
    private final int businessStreamFragmenter_maxOutstandingContextCount;
    private final int signalingPacketAssembler_maxOutstandingContextCount;
    private final int businessPacketAssembler_maxOutstandingContextCount;
    private final int businessStreamAssembler_maxOutstandingContextCount;
    private final long businessPacketAssembler_maxIdleTimeNanos;
    private final ChunkCodecRegistry chunkCodecRegistry;
    private final TaskScheduler taskScheduler;
    private final PartCodecRegistry partCodecRegistry;
    private final TimeWheel<BusinessPacketAssembler> timeWheel;

    public CodecSupplier(NexalithicBuilderContext context, NexalithicEndpoint.Modules modules, TimeWheel<BusinessPacketAssembler> timeWheel) {
        signalingPacketFragmenter_maxOutstandingContextCount = context.getOption(SignalingPacketFragmenter.OPTIONS.MaxOutstandingContextCount);
        businessPacketFragmenter_maxOutstandingContextCount = context.getOption(BusinessPacketFragmenter.OPTIONS.MaxOutstandingContextCount);
        businessStreamFragmenter_maxOutstandingContextCount = context.getOption(BusinessPacketFragmenter.OPTIONS.MaxOutstandingContextCount);
        signalingPacketAssembler_maxOutstandingContextCount = context.getOption(SignalingPacketAssembler.OPTIONS.MaxOutstandingContextCount);
        businessPacketAssembler_maxOutstandingContextCount = context.getOption(BusinessPacketAssembler.OPTIONS.MaxOutstandingContextCount);
        businessStreamAssembler_maxOutstandingContextCount = context.getOption(BusinessStreamAssembler.OPTIONS.MaxOutstandingContextCount);
        businessPacketAssembler_maxIdleTimeNanos = TimeUnit.NANOSECONDS.convert(context.getOption(BusinessPacketAssembler.OPTIONS.MaxIdleTimeMillis), TimeUnit.MILLISECONDS);
        chunkCodecRegistry = context.getModule(CodecResources.MODULES.ChunkCodecRegistry);
        taskScheduler = context.getModule(modules.TaskScheduler);
        partCodecRegistry = context.getModule(BusinessPacketAssembler.MODULES.PayloadCodecRegistry);
        this.timeWheel = timeWheel;
    }

    public SignalingPacketFragmenter createSignalingPacketFragmenter() {
        return new SignalingPacketFragmenter(signalingPacketFragmenter_maxOutstandingContextCount);
    }

    public BusinessModelFragmenter<BusinessPacket> createBusinessPacketFragmenter(AbstractSession<?> session) {
        return new BusinessPacketFragmenter(partCodecRegistry, businessPacketFragmenter_maxOutstandingContextCount,
                fragmenterBusinessModelCodecCallbackFactory(session));
    }

    public BusinessModelFragmenter<BusinessStream<?>> createBusinessStreamFragmenter(AbstractSession<?> session) {
        return new BusinessStreamFragmenter(chunkCodecRegistry, businessStreamFragmenter_maxOutstandingContextCount, fragmenterBusinessModelCodecCallbackFactory(session));
    }

    public SignalingPacketAssembler createSignalingPacketAssembler() {
        return new SignalingPacketAssembler(signalingPacketAssembler_maxOutstandingContextCount);
    }

    public BusinessModelAssembler<BusinessPacket> createBusinessPacketAssembler(AbstractSession<?> session) {
        return new BusinessPacketAssembler(partCodecRegistry, timeWheel,
                businessPacketAssembler_maxOutstandingContextCount, businessPacketAssembler_maxIdleTimeNanos,
                assemblerBusinessModelCodecCallbackFactory(session));
    }

    public BusinessModelAssembler<BusinessStream<?>> createBusinessStreamAssembler(AbstractSession<?> session) {
        return new BusinessStreamAssembler(chunkCodecRegistry, businessStreamAssembler_maxOutstandingContextCount, assemblerBusinessModelCodecCallbackFactory(session));
    }

    protected BusinessModelCodecCallback.Factory fragmenterBusinessModelCodecCallbackFactory(AbstractSession<?> session) {
        BusinessModelCodecCallback shared = (modelSequence, correlationId, way) -> {
            NexalithicTask task = session.getTaskCoordinator().find(correlationId);
            if (task != null) {
                session.getTaskCoordinator().activate(task);
            }
        };
        return (modelSequence, correlationId, way) -> {
            if (!way.isRequest()) {
                return EmptyCallback.INSTANCE;
            }
            NexalithicTask task = session.getTaskCoordinator().find(correlationId);
            if (task == null) {
                return EmptyCallback.INSTANCE;
            }
            TransferListener listener = task.getRequestListener();
            return listener == null ? shared : new TransferListenerCallback(taskScheduler, listener, shared);
        };
    }

    protected BusinessModelCodecCallback.Factory assemblerBusinessModelCodecCallbackFactory(AbstractSession<?> session) {
        return (modelSequence, correlationId, way) -> {
            if (!way.isResponse()) {
                return EmptyCallback.INSTANCE;
            }
            NexalithicTask task = session.getTaskCoordinator().find(correlationId);
            if (task == null || task.getResponseListener() == null) {
                return EmptyCallback.INSTANCE;
            }
            return new TransferListenerCallback(taskScheduler, task.getResponseListener(), null);
        };
    }

    private static class EmptyCallback implements BusinessModelCodecCallback {
        private static final EmptyCallback INSTANCE = new EmptyCallback();
        private EmptyCallback() {}
        @Override
        public void onCompleted(int modelSequence, long correlationId, BusinessModel.Way way) {}
    }

    private static class TransferListenerCallback implements BusinessModelCodecCallback {
        private final TaskScheduler taskScheduler;
        private final TransferListener transferListener;
        private final BusinessModelCodecCallback completion;
        private TransferSnapshot snapshot;
        private boolean completed;

        protected TransferListenerCallback(TaskScheduler taskScheduler, TransferListener transferListener,
                                           BusinessModelCodecCallback completion) {
            this.taskScheduler = taskScheduler;
            this.transferListener = transferListener;
            this.completion = completion;
        }

        @Override
        public void onStarted(long total) {
            if (snapshot != null || completed) {
                return;
            }
            TransferSnapshot created = new TransferSnapshot(total);
            if (taskScheduler.startTransferListener(transferListener, created)) {
                snapshot = created;
            }
        }

        @Override
        public void onAdvanced(long append) {
            if (snapshot != null && !completed) {
                snapshot.appendTransferred(append);
            }
        }

        @Override
        public void onCompleted(int modelSequence, long correlationId, BusinessModel.Way way) {
            if (completed) {
                return;
            }
            completed = true;
            try {
                if (snapshot != null) {
                    taskScheduler.finishTransferListener(transferListener);
                }
            } finally {
                if (completion != null) {
                    completion.onCompleted(modelSequence, correlationId, way);
                }
            }
        }

        @Override
        public void onDiscarded() {
            if (completed) {
                return;
            }
            completed = true;
            if (snapshot != null) {
                taskScheduler.cancelTransferListener(transferListener);
            }
        }
    }
}
