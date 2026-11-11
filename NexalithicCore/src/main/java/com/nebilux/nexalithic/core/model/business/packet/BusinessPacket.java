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

package com.nebilux.nexalithic.core.model.business.packet;

import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.packet.builtin.TextPart;

import java.util.*;

/**
 * 业务包
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BusinessPacket extends BusinessModel<BusinessPacket> {
    /**
     * 业务数据部分
     *
     * @param <T> 业务数据类型（如 Map, String 或自定义 POJO）
     * @author Reonvia
     * @since 0.1.0
     */
    public abstract static class Part<T> implements BusinessModel.Payload {
        /** 具体的业务数据对象。 */
        protected final T value;

        protected Part(T value) {
            this.value = value;
        }

        /**
         * 获取业务数据对象。
         * @return T
         */
        public final T value() {
            return value;
        }

        /** 返回业务内容的逻辑字节数，不包含编解码器元数据。 */
        public abstract long logicalSize();

        /**
         * 创建一个指向相同数据源的新实例。
         */
        public abstract Part<T> duplicate();
    }

    public static final int MAX_PART_COUNT = Byte.MAX_VALUE;

    private final long packetSize;
    private final byte partCount;
    private final List<Part<?>> parts;

    private BusinessPacket(long correlationId, Way way, short[] path, List<Part<?>> parts) {
        super(correlationId, way, path);
        this.parts = parts;
        this.partCount = (byte) parts.size();
        packetSize = logicalSize(this.parts);
    }

    public static BusinessPacket create(long correlationId, Way way, short[] path, List<Part<?>> parts) {
        return new BusinessPacket(correlationId, way, verifyAndCopyPath(path), verifyAndCopyParts(parts));
    }

    public static Builder builder(Way way, short... path) {
        return new Builder(way, path);
    }

    /**
     * 深度克隆报文结构，但保持 Payload 数据的引用。
     */
    public BusinessPacket duplicate() {
        List<Part<?>> copies = new ArrayList<>(partCount);
        for (Part<?> part : parts) {
            copies.add(part.duplicate());
        }
        return new BusinessPacket(correlationId, way, path, copies);
    }

    public final List<Part<?>> parts() {
        return parts;
    }

    @SuppressWarnings("unchecked")
    public final <P extends Part<?>> P part(int index) {
        if (parts == null || parts.isEmpty() || index >= partCount) {
            return null;
        }
        return (P) parts.get(index);
    }

    @SuppressWarnings("unchecked")
    public final <P extends Part<?>> P firstPart() {
        if (parts == null || parts.isEmpty()) {
            return null;
        }
        return (P) parts.getFirst();
    }

    @SuppressWarnings("unchecked")
    public final <P extends Part<?>> P lastPart() {
        if (parts == null || parts.isEmpty()) {
            return null;
        }
        return (P) parts.getLast();
    }

    public final byte getPartCount() {
        return partCount;
    }

    /** 返回构建时计算的业务内容逻辑字节数。 */
    public final long getPacketSize() {
        return packetSize;
    }

    public String getDisplayMessage() {
        return switch (firstPart()) {
            case null -> "";
            case TextPart tp -> tp.value();
            case Object other -> other.toString();
        };
    }

    @Override
    public Form form() {
        return Form.Packet;
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CorrelationId: ").append(getCorrelationId()).append(", Way: ").append(getWay()).append(", Path: ").append(Arrays.toString(getPath())).append(", partCount: ").append(partCount);
        if (parts != null) {
            sb.append(", Parts: { ");
            for (Part<?> part : parts) {
                sb.append(part).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append(" }");
        }
        return sb.toString();
    }

    private static long logicalSize(List<? extends Part<?>> parts) {
        long size = 0;
        for (Part<?> part : parts) {
            long partSize = part.logicalSize();
            if (partSize < 0) {
                throw new IllegalArgumentException("Part logical size must be non-negative: " + partSize);
            }
            size = Math.addExact(size, partSize);
        }
        return size;
    }

    private static List<Part<?>> verifyAndCopyParts(List<Part<?>> parts) {
        if (parts.size() > MAX_PART_COUNT) {
            throw PayloadOverflowException.exceeding(0, parts.size(), MAX_PART_COUNT);
        }
        return List.copyOf(parts);
    }

    /**
     * 可复用的建设者；仅由一个线程使用，Way 与路径在创建后保持固定。
     *
     * @author Reonvia
     * @since 0.5.0
     */
    public static class Builder extends BusinessModel.Builder<BusinessPacket> {
        private final List<Part<?>> parts = new ArrayList<>();

        protected Builder(Way way, short... path) {
            super(way, path);
        }

        public Builder part(Part<?> part) {
            Objects.requireNonNull(part, "part");
            if (parts.size() >= MAX_PART_COUNT) {
                throw PayloadOverflowException.exceeding(parts.size(), 1, MAX_PART_COUNT);
            }
            parts.add(part);
            return this;
        }

        public Builder parts(Part<?>... values) {
            return parts(List.of(Objects.requireNonNull(values, "values")));
        }

        public Builder parts(Collection<? extends Part<?>> values) {
            List<? extends Part<?>> snapshot = List.copyOf(Objects.requireNonNull(values, "values"));
            if (snapshot.size() > MAX_PART_COUNT - parts.size()) {
                throw PayloadOverflowException.exceeding(parts.size(), snapshot.size(), MAX_PART_COUNT);
            }
            parts.addAll(snapshot);
            return this;
        }

        @Override
        public void reset() {
            parts.clear();
        }

        @Override
        public BusinessPacket build(long correlationId) {
            return new BusinessPacket(correlationId, way, path, List.copyOf(parts));
        }
    }
}
