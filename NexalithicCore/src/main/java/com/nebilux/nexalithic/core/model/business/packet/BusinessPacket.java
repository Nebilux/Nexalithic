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

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.model.business.packet.builtin.TextPayload;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.CRC32;

/**
 * 业务包
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BusinessPacket extends BusinessModel<BusinessPacket> {
    /**
     * 业务有效载荷基类。
     * <p>本类定义了数据在业务实体（T）与二进制流（LoopBufferView）之间转换的标准流程。
     * 支持大对象的分段编解码。</p>
     *
     * @param <T> 业务数据类型（如 Map, String 或自定义 POJO）
     * @author Reonvia
     * @since 0.1.0
     */
    public abstract static class Payload<T> {
        /** 具体的业务数据对象 */
        protected volatile T value;
        /** 数据的总字节大小（用于进度控制和长度校验） */
        protected volatile long totalSize;
        /** 当前已编解码处理的字节大小 */
        protected volatile long processedSize;

        public Payload() {}

        /**
         * 设置业务数据对象。
         * @param value 数据实例
         */
        public void value(T value) {
            this.value = value;
        }

        /**
         * 获取业务数据对象。
         * @return T
         */
        public final T value() {
            return value;
        }

        /**
         * 获取 Payload 的实际总字节大小。
         * <p>在编码时，该值决定了需要写出多少数据；在解码时，该值通常由报文头通知。</p>
         * @return 字节总数
         */
        public final long getTotalSize() {
            return totalSize;
        }

        /**
         * 获取当前已完成读/写的字节数。
         * <p>在流式传输大对象（如文件）时，可用于计算进度或驱动分段写入逻辑。</p>
         * @return 已处理字节数
         */
        public final long getProcessedSize() {
            return processedSize;
        }

        /**
         * <b>编码动作：</b>将业务对象（data）转换为二进制流并写入缓冲区。
         * <p>框架可能会多次调用此方法，直到 {@code processedSize == totalSize}。
         *
         * @param output 目标输出视图
         * @return 本次实际写入的字节数
         * @throws IOException 如果业务逻辑转换出错
         */
        public abstract int encode(LoopBuffer.LimitedWritableView output) throws IOException;

        /**
         * <b>解码动作：</b>从缓冲区读取二进制数据并填充/解析为业务对象（data）。
         * <p>框架可能会多次调用此方法，直到 {@code processedSize == totalSize}。
         *
         * @param input 源输入视图
         * @return 本次实际读取并解析的字节数
         * @throws IOException 如果格式校验或解析出错
         */
        public abstract int decode(LoopBuffer.LimitedReadableView input) throws IOException;

        /**
         * 创建一个指向相同数据源，但进度重置为 0 的新实例。
         * 用于多线程并发发送同一报文。
         */
        public abstract Payload<T> duplicate();

        /**
         * 获取有效载荷的唯一标识（UID）。
         * <p>默认实现基于类名及字段结构生成 CRC32 校验码，用于协议版本识别和类型安全校验。
         * <b>建议：</b>在生产环境中重写此方法，返回一个固定的常量 {@code long} 值以提升性能。</p>
         *
         * @return 64 位唯一标识符
         */
        public long getPayloadUID() {
            Class<?> clazz = getClass();
            StringBuilder sb = new StringBuilder();
            sb.append(clazz.getName());
            Field[] fields = clazz.getDeclaredFields();
            Arrays.sort(fields, Comparator.comparing(Field::getName));
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic()) {
                    sb.append("|").append(field.getName()).append(":").append(field.getType().getSimpleName());
                }
            }
            CRC32 crc = new CRC32();
            crc.update(sb.toString().getBytes(StandardCharsets.UTF_8));
            return crc.getValue();
        }

        /**
         * 编码准备钩子。
         * <p>在第一次调用 {@link #encode(LoopBuffer.LimitedWritableView)} 之前触发。用于打开资源或初始化编码状态机。</p>
         */
        public void prepareEncode() throws IOException {}

        /**
         * 编码完成钩子。
         * <p>在所有数据编码完毕（processedSize == totalSize）后触发。</p>
         */
        public void finishEncode() throws IOException {}

        /**
         * 解码准备钩子。
         * <p>在收到报文长度并准备开始解码前触发。常用于初始化业务对象容器（如 {@code new T()}）。</p>
         *
         * @param totalSize 报文头部声明的总数据大小
         */
        public void prepareDecode(long totalSize) throws IOException {
            this.totalSize = totalSize;
        }

        /**
         * 解码完成钩子。
         * <p>在成功解析完整数据后触发。常用于执行业务逻辑前的最终校验或数据合并。</p>
         */
        public void finishDecode() throws IOException {}

        /**
         * 资源清理钩子。
         * <p>无论操作成功还是异常，在 Payload 生命周期结束时触发。
         * 务必在此释放引用的池化对象或其他外部资源（如临时文件句柄）。</p>
         */
        public void release() {}
    }

    public static final int MAX_PAYLOAD_COUNT = Byte.MAX_VALUE;
    public static final int BASE_HEADER_SIZE = Byte.BYTES * 2 + Short.BYTES + Long.BYTES * 2;
    private volatile boolean sealed = false;

    private long packetSize;
    private byte payloadCount;
    private long[] payloadsMeta;
    private List<Payload<?>> payloads;

    private BusinessPacket(Way way, short... path) {
        super(way, path);
    }
    public static BusinessPacket create(Way way, short... path) {
        if (path != null && path.length > MAX_PATH_DEPTH) {
            throw BusinessPacketException.pathDepthExceeded(MAX_PATH_DEPTH);
        }
        return new BusinessPacket(way, path);
    }

    public final BusinessPacket attach(Payload<?> payload) {
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
    public final BusinessPacket attach(Payload<?>... payloads) {
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
            for (Payload<?> payload : payloads) {
                if (payload != null) {
                    this.payloads.add(payload);
                }
            }
        }
        return this;
    }
    public final BusinessPacket attach(Collection<Payload<?>> payloads) {
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
            for (Payload<?> payload : payloads) {
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
                Payload<?> payload = payloads.get(i);
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
            for (Payload<?> p : this.payloads) {
                clone.payloads.add(p.duplicate());
            }
        }
        clone.sealed = true;
        return clone;
    }

    public final List<Payload<?>> payloads() {
        return payloads;
    }
    @SuppressWarnings("unchecked")
    public final <P extends Payload<?>> P payload(int index) {
        if (payloads == null || payloads.isEmpty() || index >= payloadCount) {
            return null;
        }
        return (P) payloads.get(index);
    }
    @SuppressWarnings("unchecked")
    public final <P extends Payload<?>> P firstPayload() {
        if (payloads == null || payloads.isEmpty()) {
            return null;
        }
        return (P) payloads.getFirst();
    }
    @SuppressWarnings("unchecked")
    public final <P extends Payload<?>> P lastPayload() {
        if (payloads == null || payloads.isEmpty()) {
            return null;
        }
        return (P) payloads.getLast();
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

    public final int getHeaderSize() {
        return BASE_HEADER_SIZE + Byte.toUnsignedInt(getPathDepth()) * Short.BYTES + payloadCount * Long.BYTES * 2;
    }

    @Override
    public Form form() {
        return Form.Packet;
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Way: ").append(getWay()).append(", Path: ").append(Arrays.toString(getPath())).append(", TaskId: ").append(getTaskId())
                .append(", PacketSize: ").append(TransferSnapshot.formatSize(packetSize)).append(", PayloadCount: ").append(payloadCount);
        if (payloads != null) {
            sb.append(", Payloads: { ");
            for (Payload<?> payload : payloads) {
                sb.append(payload).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append(" }");
        }
        return sb.toString();
    }
}
