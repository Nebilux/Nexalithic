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

import com.nebilux.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.packet.payload.AbstractPayload;
import com.nebilux.nexalithic.core.model.business.packet.payload.TextPayload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务包
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BusinessPacket extends BusinessModel<BusinessPacket> {
    public static final int MAX_PAYLOAD_COUNT = Byte.MAX_VALUE;
    public static final int BASE_HEADER_SIZE = Byte.BYTES * 2 + Short.BYTES + Long.BYTES * 2;
    private static final AtomicInteger counter = new AtomicInteger(0);
    private final int packetId = counter.getAndIncrement();
    private volatile boolean sealed = false;

    private long packetSize;
    private byte payloadCount;
    private long[] payloadsMeta;
    private List<AbstractPayload<?>> payloads;

    private BusinessPacket(Way way, short... path) {
        super(Form.Packet, way, path);
    }
    public static BusinessPacket create(Way way, short... path) {
        if (path != null && path.length > MAX_PATH_DEPTH) {
            throw BusinessPacketException.pathDepthExceeded(MAX_PATH_DEPTH);
        }
        return new BusinessPacket(way, path);
    }

    public final BusinessPacket attach(AbstractPayload<?> payload) {
        if (sealed) {
            throw BusinessPacketException.attachmentToSealedPacket();
        }
        if (payload != null) {
            if (this.payloads == null) {
                this.payloads = new ArrayList<>();
            }
            if (payloadCount + 1 > MAX_PAYLOAD_COUNT) {
                throw PayloadOverflowException.exceeding(payloadCount, 1, MAX_PAYLOAD_COUNT);
            }
            this.payloads.add(payload);
        }
        return this;
    }
    public final BusinessPacket attach(AbstractPayload<?>... payloads) {
        if (sealed) {
            throw BusinessPacketException.attachmentToSealedPacket();
        }
        if (payloads != null && payloads.length > 0) {
            if (this.payloads == null) {
                this.payloads = new ArrayList<>();
            }
            int newCount = payloads.length;
            if (payloadCount + newCount > MAX_PAYLOAD_COUNT) {
                throw PayloadOverflowException.exceeding(payloadCount, newCount, MAX_PAYLOAD_COUNT);
            }
            for (AbstractPayload<?> payload : payloads) {
                if (payload != null) {
                    this.payloads.add(payload);
                }
            }
        }
        return this;
    }
    public final BusinessPacket attach(Collection<AbstractPayload<?>> payloads) {
        if (sealed) {
            throw BusinessPacketException.attachmentToSealedPacket();
        }
        if (payloads != null && !payloads.isEmpty()) {
            if (this.payloads == null) {
                this.payloads = new ArrayList<>();
            }
            int newCount = payloads.size();
            if (payloadCount + newCount > MAX_PAYLOAD_COUNT) {
                throw PayloadOverflowException.exceeding(payloadCount, newCount, MAX_PAYLOAD_COUNT);
            }
            for (AbstractPayload<?> payload : payloads) {
                if (payload != null) {
                    this.payloads.add(payload);
                }
            }
        }
        return this;
    }

    /**
     * 封存报文：执行最后的一次性大小计算，并禁止后续修改。
     */
    public final BusinessPacket seal() {
        if (sealed) {
            return this;
        }
        long packetSize = BASE_HEADER_SIZE;
        packetSize += (long) getPathDepth() * Short.BYTES;
        if (payloads != null && !payloads.isEmpty()) {
            int count = payloads.size();
            payloadCount = (byte) count;
            payloadsMeta = new long[count * 2];
            for (int i = 0; i < count; i++) {
                AbstractPayload<?> payload = payloads.get(i);
                long payloadSize = payload.getTotalSize();
                payloadsMeta[i * 2] = payload.getPayloadUID();
                payloadsMeta[i * 2 + 1] = payloadSize;
                packetSize += payloadSize;
            }
            packetSize += (long) payloadsMeta.length * Long.BYTES;
        } else {
            payloadCount = 0;
        }
        this.packetSize = packetSize;
        this.sealed = true;
        return this;
    }

    /**
     * 深度克隆报文结构，但保持 Payload 数据的引用。
     * 专门用于 pushToAll 场景。
     */
    public BusinessPacket duplicate() {
        if (!this.sealed) {
            throw BusinessPacketException.broadcastDuplicationRequiresSealedPacket();
        }
        BusinessPacket clone = new BusinessPacket(getWay(), getPath());
        clone.setTaskId(getTaskId());
        clone.packetSize = this.packetSize;
        clone.payloadCount = this.payloadCount;
        clone.payloadsMeta = this.payloadsMeta;
        if (this.payloads != null) {
            clone.payloads = new ArrayList<>(this.payloadCount);
            for (AbstractPayload<?> p : this.payloads) {
                clone.payloads.add(p.duplicate());
            }
        }
        clone.sealed = true;
        return clone;
    }

    public final List<AbstractPayload<?>> payloads() {
        return payloads;
    }
    @SuppressWarnings("unchecked")
    public final <P extends AbstractPayload<?>> P payload(int index) {
        if (payloads == null || payloads.isEmpty() || index >= payloadCount) {
            return null;
        }
        return (P) payloads.get(index);
    }
    @SuppressWarnings("unchecked")
    public final <P extends AbstractPayload<?>> P firstPayload() {
        if (payloads == null || payloads.isEmpty()) {
            return null;
        }
        return (P) payloads.getFirst();
    }
    @SuppressWarnings("unchecked")
    public final <P extends AbstractPayload<?>> P lastPayload() {
        if (payloads == null || payloads.isEmpty()) {
            return null;
        }
        return (P) payloads.getLast();
    }
    public final int getHeaderSize() {
        return BASE_HEADER_SIZE + Byte.toUnsignedInt(getPathDepth()) * Short.BYTES + payloadCount * Long.BYTES * 2;
    }
    public final int getPacketId() {
        return packetId;
    }
    public final long getPacketSize() {
        return packetSize;
    }
    public final byte getPayloadCount() {
        return payloadCount;
    }
    public final long[] getPayloadsMeta() {
        return payloadsMeta;
    }
    public final long getTotalPayloadSize() {
        return getPacketSize() - getHeaderSize();
    }

    public String getDisplayMessage() {
        return switch (firstPayload()) {
            case null -> "";
            case TextPayload tp -> tp.value();
            case Object other -> other.toString();
        };
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Way: ").append(getWay()).append(", Path: ").append(Arrays.toString(getPath())).append(", TaskId: ").append(getTaskId())
                .append(", PacketSize: ").append(TransferSnapshot.formatSize(packetSize)).append(", PayloadCount: ").append(payloadCount);
        if (payloads != null) {
            sb.append(", Payloads: { ");
            for (AbstractPayload<?> payload : payloads) {
                sb.append(payload).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append(" }");
        }
        return sb.toString();
    }
}
