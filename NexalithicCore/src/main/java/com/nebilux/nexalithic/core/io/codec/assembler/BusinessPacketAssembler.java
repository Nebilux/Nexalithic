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
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
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
public class BusinessPacketAssembler extends BusinessModelAssembler<BusinessPacket> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, BusinessPacketAssembler.class);
    public static final class Options extends BusinessModelAssembler.Options {
        private Options(Class<?> holder) {
            super(holder);
        }

        @Override
        protected int MaxOutstandingContextCount_Value() {
            return 64;
        }
    }
    private static final int BASE_EXTRA_HEADER_SIZE = Byte.BYTES; // partCount
    private static final int PAYLOAD_METADATA_SIZE = Integer.BYTES // PartCodec ID
            + Long.BYTES; // 编码长度

    private final PartCodecRegistry partCodecRegistry;

    public BusinessPacketAssembler(SessionKey sessionKey, int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory,
                                   long maxIdleTimeNanos, PartCodecRegistry partCodecRegistry,
                                   TimeWheel<BusinessModelAssembler.CodecContext<?>> timeWheel) {
        super(sessionKey, maxOutstandingContextCount, codecCallbackFactory, maxIdleTimeNanos, timeWheel);
        this.partCodecRegistry = partCodecRegistry;
    }

    @Override
    protected boolean deliverOnFirstFrame() {
        return false;
    }

    @Override
    protected CodecContext<BusinessPacket> createCodecContext(int modelSequence, BusinessModel.Way way, short[] path, long correlationId, LoopBuffer input, int surplus) throws IOException {
        if ((surplus -= BASE_EXTRA_HEADER_SIZE) < 0) {
            throw new IOException("Business packet START payload is too short");
        }
        int partCount = input.unsafeGetByte();
        if (partCount > BusinessPacket.MAX_PART_COUNT) {
            throw new IOException("Invalid business packet part count: " + partCount);
        }
        if (surplus < partCount * PAYLOAD_METADATA_SIZE) {
            throw new IOException("Business packet model header is incomplete");
        }
        long remaining = 0;
        PartCodec.Decoder<?>[] decoders = new PartCodec.Decoder<?>[partCount];
        long[] lengths = new long[partCount];
        try {
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
                decoders[partIndex] = decoder;
                lengths[partIndex] = partSize;
                remaining = Math.addExact(remaining, partSize);
            }
            BusinessModelCodecCallback callback = codecCallbackFactory.create(modelSequence, correlationId, way);
            return new PacketCodecContext(modelSequence, callback, correlationId, way, path, decoders, lengths, remaining);
        } catch (Throwable throwable) {
            for (PartCodec.Decoder<?> decoder : decoders) {
                if (decoder != null) {
                    decoder.clear();
                }
            }
            throw throwable;
        }
    }

    private final class PacketCodecContext extends CodecContext<BusinessPacket> {
        private long correlationId;
        private BusinessModel.Way way;
        private short[] path;
        private PartCodec.Decoder<?>[] decoders;
        private long[] lengths;
        private int currentPartIndex;
        private long currentPartRemaining;
        private long remaining;
        private List<BusinessPacket.Part<?>> parts;
        private BusinessPacket packet;

        public PacketCodecContext(int modelSequence, BusinessModelCodecCallback callback, long correlationId, BusinessModel.Way way,
                                  short[] path, PartCodec.Decoder<?>[] decoders, long[] lengths, long remaining) {
            super(modelSequence, callback);
            this.correlationId = correlationId;
            this.way = way;
            this.path = path;
            this.decoders = decoders;
            this.lengths = lengths;
            this.remaining = remaining;
            this.parts = new ArrayList<>(decoders.length);
            this.currentPartRemaining = decoders.length == 0 ? 0 : lengths[0];
        }

        @Override
        protected long callbackTotalSize() {
            return COMMON_BASE_HEADER_SIZE + (long) path.length * Short.BYTES + BASE_EXTRA_HEADER_SIZE
                    + (long) decoders.length * PAYLOAD_METADATA_SIZE + remaining;
        }

        @Override
        protected void readPayload(LoopBuffer input, int quota) throws IOException {
            while (currentPartIndex < decoders.length && (quota > 0 || currentPartRemaining == 0)) {
                PartCodec.Decoder<?> decoder = decoders[currentPartIndex];
                if (currentPartRemaining > 0) {
                    int consumed = decode(decoder, currentPartRemaining, input, quota);
                    if (consumed == 0) {
                        break;
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
                    if (++currentPartIndex < decoders.length) {
                        currentPartRemaining = lengths[currentPartIndex];
                    }
                }
            }
        }

        @Override
        protected boolean tryCompleteDecoding() throws IOException {
            if (remaining != 0 || currentPartIndex != decoders.length) {
                throw new IOException("Business packet ended before all Parts completed");
            }
            packet = BusinessPacket.create(correlationId, way, path, parts);
            return true;
        }

        @Override
        public BusinessPacket model() {
            return packet;
        }

        @Override
        protected void onComplete() {
            onDiscard();
        }

        @Override
        protected void onDiscard() {
            for (; currentPartIndex < decoders.length; currentPartIndex++) {
                decoders[currentPartIndex].clear();
            }
        }
    }
}
