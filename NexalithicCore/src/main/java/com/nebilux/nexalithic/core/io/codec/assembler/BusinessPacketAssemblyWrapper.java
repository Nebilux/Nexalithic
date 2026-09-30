package com.nebilux.nexalithic.core.io.codec.assembler;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.SelfStaticRecyclableWrapper;
import com.nebilux.nexalithic.core.io.codec.CodecCallback;
import com.nebilux.nexalithic.core.messaging.payload.PayloadRegistry;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.business.payload.AbstractPayload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 业务包组装封装器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/15
 * @version 1.0.0
 */
public class BusinessPacketAssemblyWrapper extends SelfStaticRecyclableWrapper<BusinessPacketAssemblyWrapper> {
    public record Constant(long MaxIdleTimeNanos) {}
    private final Constant CONSTANT;
    private final CodecCallback codecCallback;
    private final PacketBuilder packetBuilder = new PacketBuilder();
    private final PayloadRegistry payloadRegistry;
    private BusinessPacket packet;
    private long remaining;
    private int packetId;
    private int payloadIndex;
    private boolean headerRead;
    private long lastActiveTimeNanos;

    public BusinessPacketAssemblyWrapper(GenericWrapperPool<BusinessPacketAssemblyWrapper, BusinessPacketAssemblyWrapper> owner,
                                         Constant constant, CodecCallback codecCallback, PayloadRegistry payloadRegistry) {
        super(owner);
        CONSTANT = constant;
        this.codecCallback = codecCallback;
        this.payloadRegistry = payloadRegistry;
    }

    public boolean hasFrame() {
        if (remaining > 0) {
            return true;
        } else {
            packet = packetBuilder.build();
            codecCallback.complete();
            return false;
        }
    }

    public int onFrame(LoopBuffer input, int quota, boolean isStartFrame) throws IOException {
        updateLastActiveTime();
        int total = 0;
        if (!headerRead) {
            if (!isStartFrame) {
                input.advanceHead(quota);
                return quota;
            }
            total += readPacketHeader(input);
            codecCallback.prepare(packetBuilder.taskId, PacketBuilder.WAYS[packetBuilder.way]);
            codecCallback.start(remaining);
            headerRead = true;
        }
        List<AbstractPayload<?>> payloads = packetBuilder.payloads;
        int read;
        while (total < quota && payloadIndex < packetBuilder.payloadCount) {
            AbstractPayload<?> payload = payloads.get(payloadIndex);
            long totalSize = packetBuilder.payloadsMeta[payloadIndex * 2 + 1];
            long processedSize = payload.getProcessedSize();
            LoopBuffer.LimitedReadableView readableView = input.unsafeLimitedReadableView(
                    Math.toIntExact(Math.min(totalSize - processedSize, quota - total))
            );
            try {
                if (processedSize == 0) {
                    payload.prepareDecode(totalSize);
                }
                read = payload.decode(readableView);
                if (payload.getProcessedSize() >= totalSize) {
                    payload.finishDecode();
                    payload.release();
                    payloadIndex++;
                }
                total += read;
            } catch (IOException e) {
                payload.release();
                throw e;
            }
        }
        remaining -= total;
        codecCallback.update(remaining);
        return total;
    }
    private int readPacketHeader(LoopBuffer input) throws IOException {
        long taskId = input.unsafeGetLong();
        long packetSize = input.unsafeGetLong();
        short way = input.unsafeGetShort();
        byte depth = input.unsafeGetByte();
        short[] path = null;
        int read = BusinessPacket.BASE_HEADER_SIZE;
        if (depth > 0) {
            path = new short[depth];
            for (int i = 0; i < depth; i++) {
                path[i] = input.unsafeGetShort();
            }
            read += depth * Short.BYTES;
        }
        byte count = input.unsafeGetByte();
        List<AbstractPayload<?>> payloads = null;
        long[] payloadsMeta = null;
        if (count > 0) {
            payloads = new ArrayList<>(count);
            payloadsMeta = new long[count * 2];
            for (int i = 0; i < count; i++) {
                long uid = input.unsafeGetLong();
                long size = input.unsafeGetLong();
                AbstractPayload<?> payload = payloadRegistry.get(uid);
                if (payload == null) {
                    throw new IOException("Unknown Payload UID: " + uid);
                }
                payloads.add(payload);
                payloadsMeta[i * 2] = uid;
                payloadsMeta[i * 2 + 1] = size;
            }
            read += count * Long.BYTES * 2;
        }
        this.remaining = packetSize;
        packetBuilder.taskId = taskId;
        packetBuilder.packetSize = packetSize;
        packetBuilder.way = way;
        packetBuilder.pathDepth = depth;
        packetBuilder.path = path;
        packetBuilder.payloadCount = count;
        packetBuilder.payloads = payloads;
        packetBuilder.payloadsMeta = payloadsMeta;
        return read;
    }

    public BusinessPacketAssemblyWrapper setPacketId(int packetId) {
        this.packetId = packetId;
        return this;
    }
    public int getPacketId() {
        return packetId;
    }

    public BusinessPacket getPacket() {
        return packet;
    }

    public CodecCallback getCodecCallback() {
        return codecCallback;
    }

    public long getExpiryTimeNanos() {
        return lastActiveTimeNanos + CONSTANT.MaxIdleTimeNanos;
    }

    void updateLastActiveTime() {
        lastActiveTimeNanos = System.nanoTime();
    }

    @Override
    protected void onReset() {
        codecCallback.clear();
        packetBuilder.clear();
        packet = null;
        remaining = 0;
        packetId = 0;
        headerRead = false;
        payloadIndex = 0;
        lastActiveTimeNanos = -1;
    }

    private static class PacketBuilder {
        private static final BusinessPacket.Way[] WAYS = BusinessPacket.Way.values();
        public long taskId;
        public long packetSize;
        public short way;
        public byte pathDepth;
        public short[] path;
        public byte payloadCount;
        public long[] payloadsMeta;
        public List<AbstractPayload<?>> payloads;

        public BusinessPacket build() {
            BusinessPacket packet = BusinessPacket.create(WAYS[way], path).attach(payloads).setTaskId(taskId).seal();
            if (packetSize != packet.getPacketSize()) {
                throw new IllegalStateException("Packet Size Mismatch, %d or %d".formatted(packetSize, packet.getPacketSize()));
            }
            if (payloadsMeta != null) {
                for (int i = 0; i < payloadsMeta.length; i++) {
                    long[] payloadsMeta = packet.getPayloadsMeta();
                    if (this.payloadsMeta[i] != payloadsMeta[i]) {
                        throw new IllegalStateException("Payloads Meta Mismatch");
                    }
                }
            }
            return packet;
        }

        public void clear() {
            taskId = 0;
            packetSize = 0;
            way = 0;
            pathDepth = 0;
            path = null;
            payloadCount = 0;
            payloadsMeta = null;
            payloads = null;
        }
    }
}
