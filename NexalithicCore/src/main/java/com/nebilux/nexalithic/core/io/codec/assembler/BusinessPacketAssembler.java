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

import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.infra.timer.TimerContext;
import com.nebilux.nexalithic.core.infra.timer.TimerCoordinator;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
import com.nebilux.nexalithic.core.io.codec.CodecFrame;
import com.nebilux.nexalithic.core.lifecycle.SharedLifecycle;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodec;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodecRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 业务包汇编器。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BusinessPacketAssembler extends BusinessModelAssembler<BusinessPacket> implements TimerCoordinator<BusinessPacketAssembler> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, BusinessPacketAssembler.class);
    public static final class Options extends BusinessModelAssembler.Options {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {
            protected NexalithicOption<Integer> SlotCount() {
                return defineOptionLazy(context ->
                                Math.toIntExact(context.getOption(OPTIONS.MaxIdleTimeMillis) / context.getOption(OPTIONS.TimeWheel.TickMillis)) + 1
                        , OptionValidator.positive()
                );
            }
        };
        public final NexalithicOption<Long> MaxIdleTimeMillis = defineOption(
                30_000L, OptionValidator.positive()
        );

        private Options(Class<?> holder) {
            super(holder);
        }

        @Override
        protected int MaxOutstandingContextCount_Value() {
            return 64;
        }
    }
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<PartCodecRegistry> PayloadCodecRegistry = defineModule(PartCodecRegistry.class);
        public final NexalithicModule<SharedLifecycle<TimeWheel<BusinessPacketAssembler>>> SharedTimeWheel =
                defineModule(SharedLifecycle.class, null, TimeWheel.class.getSimpleName());

        private Modules() {
            super(BusinessPacketAssembler.class);
        }
    }

    private static final int BASE_EXTRA_HEADER_SIZE = Byte.BYTES; // partCount
    private static final int PAYLOAD_METADATA_SIZE = Integer.BYTES // PartCodec ID
            + Long.BYTES; // 编码长度

    private final PartCodecRegistry partCodecRegistry;
    private final TimeWheel<BusinessPacketAssembler> timeWheel;
    private final long maxIdleTimeNanos;

    public BusinessPacketAssembler(PartCodecRegistry partCodecRegistry, TimeWheel<BusinessPacketAssembler> timeWheel,
                                   int maxOutstandingContextCount, long maxIdleTimeNanos, BusinessModelCodecCallback.Factory codecCallbackFactory) {
        super(maxOutstandingContextCount, codecCallbackFactory);
        this.partCodecRegistry = partCodecRegistry;
        this.timeWheel = timeWheel;
        this.maxIdleTimeNanos = maxIdleTimeNanos;
    }

    @Override
    protected CodecContext<BusinessPacket> createCodecContext(int modelSequence, BusinessModel.Way way, short[] path, long correlationId, LoopBuffer input, int surplus) throws IOException {
        if ((surplus -= BASE_EXTRA_HEADER_SIZE) < 0) {
            throw new IOException("Business packet START payload is too short");
        }
        int partCount = Byte.toUnsignedInt(input.unsafeGetByte());
        if (partCount > BusinessPacket.MAX_PART_COUNT) {
            throw new IOException("Invalid business packet part count: " + partCount);
        }
        if (surplus != partCount * PAYLOAD_METADATA_SIZE) {
            throw new IOException("Business packet START payload length mismatch");
        }
        long remaining = 0;
        PartCodec.Decoder<?>[] decoderArray = new PartCodec.Decoder<?>[partCount];
        long[] partSizeArray = new long[partCount];
        for (int partIndex = 0; partIndex < partCount; partIndex++) {
            int codecId = input.unsafeGetInt();
            long partSize = input.unsafeGetLong();
            if (partSize < 0) {
                throw new IOException("Negative partSize: " + partSize);
            }
            PartCodec<?> codec = partCodecRegistry.get(codecId);
            if (codec == null) {
                throw new IOException("Unknown PartCodec ID: " + codecId);
            }
            PartCodec.Decoder<?> decoder = codec.decoder();
            decoder.prepare(partSize);
            decoderArray[partIndex] = decoder;
            partSizeArray[partIndex] = partSize;
            remaining = Math.addExact(remaining, partSize);
        }
        BusinessModelCodecCallback callback = codecCallbackFactory.create(modelSequence, correlationId, way);
        return new PacketCodecContext(modelSequence, callback, correlationId, way, path, decoderArray, partSizeArray, remaining);
    }

    @Override
    protected void onContextRegistered(CodecContext<BusinessPacket> context) {
        timeWheel.schedule(this, this);
    }

    @Override
    public long getExpiryTimeNanos(TimerContext<BusinessPacketAssembler> timer) {
        PacketCodecContext context = activeFor(timer.targetStamp());
        return context == null ? Long.MIN_VALUE : context.expiryTimeNanos();
    }

    @Override
    public boolean isCancelled(TimerContext<BusinessPacketAssembler> timer) {
        PacketCodecContext context = activeFor(timer.targetStamp());
        return context == null || context.isComplete();
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<BusinessPacketAssembler> timer) {
        PacketCodecContext context = activeFor(timer.targetStamp());
        if (context == null) {
            return true;
        }
        synchronized (context) {
            if (context.isComplete()) {
                return true;
            }
            if (System.nanoTime() < context.expiryTimeNanos()) {
                return false;
            }
            if (removeContext(context.modelSequence(), context)) {
                context.discard();
            }
        }
        return true;
    }

    private PacketCodecContext activeFor(long timerStamp) {
        CodecContext<BusinessPacket> context = getContext((int) timerStamp);
        if (context instanceof PacketCodecContext packetContext) {
            return packetContext;
        }
        return null;
    }

    private final class PacketCodecContext extends CodecContext<BusinessPacket> {
        private long correlationId;
        private BusinessModel.Way way;
        private short[] path;
        private PartCodec.Decoder<?>[] decoderArray;
        private long[] partSizeArray;
        private int currentPartIndex;
        private long currentPartRemaining;
        private long remaining;
        private List<BusinessPacket.Part<?>> parts;
        private BusinessPacket packet;

        private volatile long lastActiveTimeNanos;

        public PacketCodecContext(int modelSequence, BusinessModelCodecCallback callback, long correlationId, BusinessModel.Way way,
                                  short[] path, PartCodec.Decoder<?>[] decoderArray, long[] partSizeArray, long remaining) {
            super(modelSequence, callback);
            this.correlationId = correlationId;
            this.way = way;
            this.path = path;
            this.decoderArray = decoderArray;
            this.partSizeArray = partSizeArray;
            this.remaining = remaining;
            this.parts = new ArrayList<>(decoderArray.length);
            this.currentPartRemaining = decoderArray.length == 0 ? 0 : partSizeArray[0];
            lastActiveTimeNanos = System.nanoTime();
            if (remaining == 0) {
                packet = BusinessPacket.create(way, path, correlationId, parts);
            }
        }

        @Override
        protected boolean readNextFrame(CodecFrame.Type type, LoopBuffer input, int quota) throws IOException {
            if (packet != null || quota == 0 || quota > remaining) {
                throw new IOException("Invalid business packet DATA frame");
            }
            lastActiveTimeNanos = System.nanoTime();
            while (currentPartIndex < decoderArray.length && (quota > 0 || currentPartRemaining == 0)) {
                PartCodec.Decoder<?> decoder = decoderArray[currentPartIndex];
                if (currentPartRemaining > 0) {
                    int limit = Math.toIntExact(Math.min(currentPartRemaining, quota));
                    LoopBuffer.LimitedReadableView view = input.unsafeLimitedReadableView(limit);
                    decoder.decode(view);
                    int consumed = limit - view.remaining();
                    if (consumed == 0) {
                        throw new IOException("Part decoder made no progress");
                    }
                    currentPartRemaining -= consumed;
                    remaining -= consumed;
                    quota -= consumed;
                }
                if (currentPartRemaining == 0) {
                    BusinessPacket.Part<?> part = decoder.tryFinish();
                    if (part == null) {
                        throw new IOException("Part decoder did not finish at the declared length");
                    }
                    parts.add(part);
                    decoder.clear();
                    if (++currentPartIndex < decoderArray.length) {
                        currentPartRemaining = partSizeArray[currentPartIndex];
                    }
                }
            }
            if (remaining == 0) {
                if (currentPartIndex != decoderArray.length) {
                    throw new IOException("Business packet ended before all Parts completed");
                }
                packet = BusinessPacket.create(way, path, correlationId, parts);
            }
            return true;
        }

        private long expiryTimeNanos() {
            return lastActiveTimeNanos + maxIdleTimeNanos;
        }

        @Override
        protected long callbackTotalSize() {
            return COMMON_BASE_HEADER_SIZE + (long) path.length * Short.BYTES + BASE_EXTRA_HEADER_SIZE
                    + (long) decoderArray.length * PAYLOAD_METADATA_SIZE + remaining;
        }

        @Override
        public BusinessPacket model() {
            return packet;
        }

        @Override
        protected boolean isComplete() {
            return packet != null;
        }

        @Override
        protected void onComplete() {
            onDiscard();
        }

        @Override
        protected void onDiscard() {
            for (; currentPartIndex < decoderArray.length; currentPartIndex++) {
                decoderArray[currentPartIndex].clear();
            }
        }
    }
}
