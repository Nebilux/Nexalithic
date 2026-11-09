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

import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.io.codec.BusinessModelCodecCallback;
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodec;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodecRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/** 业务数据包碎片器。 */
public class BusinessPacketFragmenter extends BusinessModelFragmenter<BusinessPacket> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, BusinessPacketFragmenter.class);
    public static final class Options extends BusinessModelFragmenter.Options {
        private Options(Class<?> holder) {
            super(holder);
        }

        @Override
        protected int MaxOutstandingContextCount_Value() {
            return 64;
        }
    }

    private static final int BASE_EXTRA_HEADER_SIZE = Byte.BYTES; // partCount
    private static final int PART_METADATA_SIZE = Integer.BYTES // PartCodec ID
            + Long.BYTES; // 编码长度

    private final PartCodecRegistry partCodecRegistry;

    public BusinessPacketFragmenter(PartCodecRegistry partCodecRegistry, int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory) {
        super(maxOutstandingContextCount, codecCallbackFactory);
        this.partCodecRegistry = partCodecRegistry;
    }

    @Override
    protected CodecContext<BusinessPacket> createCodecContext(BusinessPacket model, int modelSequence) {
        List<BusinessPacket.Part<?>> parts = model.parts();
        int count = model.getPartCount();
        PartCodec.Encoder<?>[] encoders = new PartCodec.Encoder<?>[count];
        int[] codecIds = new int[count];
        long[] lengths = new long[count];
        long totalSize = COMMON_BASE_HEADER_SIZE + (long) model.getPathDepth() * Short.BYTES
                + BASE_EXTRA_HEADER_SIZE + (long) count * PART_METADATA_SIZE;
        try {
            for (int i = 0; i < count; i++) {
                encoders[i] = prepare(parts.get(i), codecIds, lengths, i, partCodecRegistry);
                totalSize = Math.addExact(totalSize, lengths[i]);
            }
            BusinessModelCodecCallback callback = codecCallbackFactory.create(modelSequence, model.getCorrelationId(), model.getWay());
            return new PacketCodecContext(callback, model, modelSequence, encoders, codecIds, lengths, totalSize);
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static <P extends BusinessPacket.Part<?>> PartCodec.Encoder<P> prepare(P part, int[] codecIds, long[] lengths, int index, PartCodecRegistry registry) throws IOException {
        PartCodec<?> found = registry.get(part.getClass());
        if (found == null || found.clazz() != part.getClass()) {
            throw new IOException("No PartCodec for " + part.getClass().getName());
        }
        PartCodec<P> codec = (PartCodec<P>) found;
        PartCodec.Encoder<P> encoder = codec.encoder();
        try {
            long size = encoder.prepare(part);
            if (size < 0) {
                throw new IOException("Negative encoded part length: " + size);
            }
            codecIds[index] = codec.id();
            lengths[index] = size;
            return encoder;
        } catch (Throwable failure) {
            encoder.clear();
            throw failure;
        }
    }

    private static final class PacketCodecContext extends CodecContext<BusinessPacket> {
        /** 三者按相同索引对应一个 Payload。 */
        private final PartCodec.Encoder<?>[] encoders;
        private final int[] codecIds;
        private final long[] lengths;
        private final long totalSize;
        private long remaining;
        /** 仅当前 Payload 需要记录尚未编码的字节数。 */
        private long currentPartRemaining;
        private int currentPartIndex;

        private PacketCodecContext(BusinessModelCodecCallback callback, BusinessPacket model, int sequence,
                                   PartCodec.Encoder<?>[] encoders, int[] codecIds, long[] lengths, long totalSize) {
            super(sequence, callback, model);
            this.encoders = encoders;
            this.codecIds = codecIds;
            this.lengths = lengths;
            this.totalSize = totalSize;
            remaining = totalSize;
            currentPartRemaining = lengths.length == 0 ? 0 : lengths[0];
        }

        @Override
        protected long callbackTotalSize() {
            return totalSize;
        }

        @Override
        protected int extraHeaderSize() {
            return BASE_EXTRA_HEADER_SIZE + encoders.length * PART_METADATA_SIZE;
        }

        @Override
        protected void writeExtraFirstFrame(LoopBuffer output) {
            output.put((byte) encoders.length);
            for (int i = 0; i < encoders.length; i++) {
                output.put(codecIds[i]);
                output.put(lengths[i]);
            }
        }

        @Override
        protected void onFirstFrameCommitted(int advanced) {
            remaining -= advanced;
        }

        @Override
        protected void writeNextFrame(LoopBuffer output, int quota) throws IOException {
            while (currentPartIndex < encoders.length) {
                PartCodec.Encoder<?> encoder = encoders[currentPartIndex];
                if (currentPartRemaining > 0) {
                    int limit = Math.toIntExact(Math.min(currentPartRemaining, quota));
                    LoopBuffer.LimitedWritableView view = output.unsafeLimitedWritableView(limit);
                    encoder.encode(view);
                    int consumed = limit - view.remaining();
                    if (consumed == 0) {
                        break;
                    }
                    currentPartRemaining -= consumed;
                    quota -= consumed;
                }
                if (currentPartRemaining == 0) {
                    if (!encoder.tryFinish()) {
                        throw new IOException("Part encoder did not finish at the declared length");
                    }
                    encoder.clear();
                    if (++currentPartIndex < encoders.length) {
                        currentPartRemaining = lengths[currentPartIndex];
                    }
                }
                if (quota == 0) {
                    break;
                }
            }
        }

        @Override
        protected void onNextFrameCommitted(int advanced) {
            remaining -= advanced;
        }

        @Override
        protected boolean isComplete() {
            return remaining == 0;
        }

        @Override
        protected boolean isReady() {
            return !isComplete();
        }

        @Override
        protected void onDiscard() {
            for (; currentPartIndex < encoders.length; currentPartIndex++) {
                encoders[currentPartIndex].clear();
            }
        }
    }
}
