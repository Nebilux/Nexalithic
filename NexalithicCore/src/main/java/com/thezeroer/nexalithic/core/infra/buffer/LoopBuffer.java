package com.thezeroer.nexalithic.core.infra.buffer;

import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.SelfStaticRecyclableWrapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * <h3>环形缓冲区</h3>
 * <li><b>零 GC 开销：</b>内部持有并复用 ByteBuffer 视图数组，避免在 I/O 循环中产生临时对象。</li>
 * <li><b>位运算索引：</b>强制容量为 2 的幂（Power of 2），利用位掩码快速计算回绕偏移。</li>
 * <li><b>Scatter/Gather 优化：</b>原生支持 {@code ByteBuffer[]} 视图，适配系统级分散/聚集 I/O。</li>
 * <li><b>单线程优化：</b>移除了非必要的 Volatile 语义，完美适配 Reactor 或 EventLoop 线程模型。</li>
 * </ul>
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/02/13
 */
@SuppressWarnings("UnusedReturnValue")
public class LoopBuffer extends SelfStaticRecyclableWrapper<LoopBuffer> {
    /** 原始底层缓冲区 */
    private final ByteBuffer buffer;
    /** 复用的可读段视图（处理回绕时包含两段） */
    private final ByteBuffer[] readViews = new ByteBuffer[2];
    /** 复用的可写段视图（处理回绕时包含两段） */
    private final ByteBuffer[] writeViews = new ByteBuffer[2];

    private final LimitedReadableView limitedReadableView = new LimitedReadableView();
    private final LimitedWritableView limitedWritableView = new LimitedWritableView();

    private final int capacity, mask;
    private long tail, head;
    private long markedTail = -1, markedHead = -1; // -1 表示当前没有标记

    /**
     * 初始化环形缓冲区。
     * @param buffer 外部分配的底层缓冲区，其 {@code remaining()} 必须为 2 的幂。
     * @throws IllegalArgumentException 如果容量不是 2 的幂。
     */
    public LoopBuffer(GenericWrapperPool<LoopBuffer, LoopBuffer> owner, ByteBuffer buffer) {
        super(owner);
        this.capacity = buffer.remaining();
        if ((capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("Capacity must be a power of 2");
        }
        mask = capacity - 1;
        this.buffer = buffer;
        for (int i = 0; i < 2; i++) {
            this.readViews[i] = buffer.duplicate();
            this.writeViews[i] = buffer.duplicate();
        }
    }

    public LimitedReadableView unsafeLimitedReadableView(int quota) {
        limitedReadableView.quota = quota;
        return limitedReadableView;
    }
    public LimitedWritableView unsafeLimitedWritableView(int quota) {
        limitedWritableView.quota = quota;
        return limitedWritableView;
    }

    /**
     * 从通道读取数据并填充到缓冲区。
     * @param channel 数据源通道。
     * @return 实际读取的总字节数。
     * @throws IOException 如果 I/O 发生错误。
     */
    public long readFromChannel(ScatteringByteChannel channel) throws IOException {
        if (isFull()) {
            return 0;
        }
        long bytesRead = channel.read(writableViews());
        if (bytesRead > 0) {
            tail += bytesRead;
        }
        return bytesRead;
    }
    /**
     * 从通道读取数据，并限制最大读取量
     * @param channel 数据源通道。
     * @param limit 最大允许读取的字节数。
     * @return 实际读取的总字节数。
     */
    public long readFromChannel(ScatteringByteChannel channel, int limit) throws IOException {
        if (isFull() || limit <= 0) {
            return 0;
        }
        long bytesRead = channel.read(applyLimit(writableViews(), limit));
        if (bytesRead > 0) {
            tail += bytesRead;
        }
        return bytesRead;
    }
    /**
     * 将缓冲区中的数据写入到通道。
     * @param channel 目标输出通道。
     * @return 实际写入的总字节数。
     * @throws IOException 如果 I/O 发生错误。
     */
    public long writeToChannel(GatheringByteChannel channel) throws IOException {
        if (isEmpty()) {
            return 0;
        }
        long bytesWritten = channel.write(readableViews());
        if (bytesWritten > 0) {
            head += bytesWritten;
        }
        return bytesWritten;
    }
    /**
     * 将缓冲区数据写入通道，并限制最大写入量
     * @param channel 目标输出通道。
     * @param limit 最大允许写入的字节数。
     * @return 实际写入的总字节数。
     */
    public long writeToChannel(GatheringByteChannel channel, int limit) throws IOException {
        if (isEmpty() || limit <= 0) {
            return 0;
        }
        long bytesWritten = channel.write(applyLimit(readableViews(), limit));
        if (bytesWritten > 0) {
            head += bytesWritten;
        }
        return bytesWritten;
    }

    /**
     * 获取当前可读区域的视图数组。
     * <p>
     * <b>注意：</b>如果发生回绕，数组将包含两个分段；否则第二段为空。
     * @return ByteBuffer 数组，视图共享底层内存。
     */
    public ByteBuffer[] readableViews() {
        int hIdx = (int) (head & mask);
        int tIdx = (int) (tail & mask);
        if (isEmpty()) {
            readViews[0].limit(0).position(0);
            readViews[1].limit(0).position(0);
        } else if (tIdx > hIdx) {
            // 数据段连续：[head, tail]
            readViews[0].limit(tIdx).position(hIdx);
            readViews[1].limit(0).position(0);
        } else {
            // 数据段回绕：[head, capacity] 和 [0, tail]
            readViews[0].limit(capacity).position(hIdx);
            readViews[1].limit(tIdx).position(0);
        }
        return readViews;
    }
    /**
     * 获取当前空闲可写区域的视图数组。
     * @return ByteBuffer 数组，可供写入。
     */
    public ByteBuffer[] writableViews() {
        int hIdx = (int) (head & mask);
        int tIdx = (int) (tail & mask);
        if (isFull()) {
            writeViews[0].limit(0).position(0);
            writeViews[1].limit(0).position(0);
        } else if (hIdx > tIdx) {
            // 空闲段连续：[tail, head]
            writeViews[0].limit(hIdx).position(tIdx);
            writeViews[1].limit(0).position(0);
        } else {
            // 空闲段回绕：[tail, capacity] 和 [0, head]
            writeViews[0].limit(capacity).position(tIdx);
            writeViews[1].limit(hIdx).position(0);
        }
        return writeViews;
    }

    /**
     * 手动向前推进写入指针。在对{@link #writableViews()}返回的views进行写入后调用。
     * @param amount 推进的字节数。
     */
    public void advanceTail(int amount) {
        this.tail += amount;
    }
    /**
     * 手动向前推进读取指针。在对{@link #readableViews()}返回的views进行读取后调用。
     * @param amount 推进的字节数。
     */
    public void advanceHead(int amount) {
        this.head += amount;
    }

    /**
     * 记录当前读位置
     */
    public void markHead() {
        markedHead = head;
    }
    /**
     * 将读位置回滚到上次标记的地方
     */
    public void resetHead() {
        if (markedHead != -1) {
            head = markedHead;
            markedHead = -1;
        }
    }
    /**
     * 丢弃标记
     */
    public void dropMarkHead() {
        markedHead = -1;
    }

    public void markTail() {
        markedTail = tail;
    }
    public void resetTail() {
        if (markedTail != -1) {
            tail = markedTail;
            markedTail = -1;
        }
    }
    public void dropMarkTail() {
        markedTail = -1;
    }

    public long getMarkedTail() {
        return markedTail;
    }
    public long getMarkedHead() {
        return markedHead;
    }
    public long getTail() {
        return tail;
    }
    public long getHead() {
        return head;
    }
    public void setHead(long head) {
        this.head = head;
    }
    public void setTail(long tail) {
        this.tail = tail;
    }

    /** 是否为空 */
    public boolean isEmpty() {
        return tail == head;
    }
    /** 是否已满 */
    public boolean isFull() {
        return tail - head >= capacity;
    }

    /** 获取可读字节总数 */
    public int readableBytes() {
        return (int) (tail - head);
    }
    /** 获取剩余可写空间 */
    public int writableBytes() {
        return capacity - readableBytes();
    }

    /**
     * 尝试重置指针索引。
     * <p>
     * 当缓冲区为空时，将指针重置为 0，这能确保下一次写入拥有最大的连续内存空间，
     * 有效避免因回绕导致的 I/O 段碎片化。
     */
    public void tryReset() {
        if (isEmpty()) {
            head = 0;
            tail = 0;
        }
    }

    public void clear() {
        tail = 0;
        head = 0;
        markedHead = -1;
        markedTail = -1;
    }

    public LoopBuffer put(byte value) {
        if (isFull()) {
            throwOverflow(Byte.BYTES);
        }
        unsafePut(value);
        return this;
    }
    public LoopBuffer put(short value) {
        if (writableBytes() < Short.BYTES) {
            throwOverflow(Short.BYTES);
        }
        unsafePut(value);
        return this;
    }
    public LoopBuffer put(int value) {
        if (writableBytes() < Integer.BYTES) {
            throwOverflow(Integer.BYTES);
        }
        unsafePut(value);
        return this;
    }
    public LoopBuffer put(long value) {
        if (writableBytes() < Long.BYTES) {
            throwOverflow(Long.BYTES);
        }
        unsafePut(value);
        return this;
    }
    public LoopBuffer put(float value) {
        return put(Float.floatToRawIntBits(value));
    }
    public LoopBuffer put(double value) {
        return put(Double.doubleToRawLongBits(value));
    }
    public LoopBuffer put(byte[] value) {
        int len = value.length;
        if (writableBytes() < len) {
            throwOverflow(len);
        }
        unsafePut(value, len);
        return this;
    }
    public LoopBuffer put(byte[] value, int offset, int length) {
        if (writableBytes() < length) {
            throwOverflow(length);
        }
        unsafePut(value, offset, length);
        return this;
    }

    public byte getByte() {
        if (isEmpty()) {
            throwUnderflow(Byte.BYTES);
        }
        return unsafeGetByte();
    }
    public short getShort() {
        if (readableBytes() < Short.BYTES) {
            throwUnderflow(Short.BYTES);
        }
        return unsafeGetShort();
    }
    public int getInt() {
        if (readableBytes() < Integer.BYTES) {
            throwUnderflow(Integer.BYTES);
        }
        return unsafeGetInt();
    }
    public long getLong() {
        if (readableBytes() < Long.BYTES) {
            throwUnderflow(Long.BYTES);
        }
        return unsafeGetLong();
    }
    public float getFloat() {
        return Float.intBitsToFloat(getInt());
    }
    public double getDouble() {
        return Double.longBitsToDouble(getLong());
    }
    public byte[] getBytes(int length) {
        byte[] bytes = new byte[length];
        getBytes(bytes);
        return bytes;
    }
    public void getBytes(byte[] dst) {
        int len = dst.length;
        if (readableBytes() < len) {
            throwUnderflow(len);
        }
        unsafeGetBytes(dst, len);
    }
    public void getBytes(byte[] dst, int offset, int length) {
        if (readableBytes() < length) {
            throwUnderflow(length);
        }
        unsafeGetBytes(dst, offset, length);
    }

    public void unsafePut(byte value) {
        buffer.put((int) (tail & mask), value);
        tail += Byte.BYTES;
    }
    public void unsafePut(short value) {
        int writePos = (int) (tail & mask);
        if (writePos <= capacity - Short.BYTES) {
            buffer.putShort(writePos, value);
        } else {
            buffer.put(writePos, (byte) (value >>> 8));
            buffer.put(0, (byte) (value & 0xFF));
        }
        tail += Short.BYTES;
    }
    public void unsafePut(int value) {
        int writePos = (int) (tail & mask);
        if (writePos <= capacity - Integer.BYTES) {
            buffer.putInt(writePos, value);
        } else {
            for (int i = 0; i < Integer.BYTES; i++) {
                buffer.put((int) ((tail + i) & mask), (byte) (value >>> (8 * (3 - i))));
            }
        }
        tail += Integer.BYTES;
    }
    public void unsafePut(long value) {
        int writePos = (int) (tail & mask);
        if (writePos <= capacity - Long.BYTES) {
            buffer.putLong(writePos, value);
        } else {
            for (int i = 0; i < Long.BYTES; i++) {
                buffer.put((int) ((tail + i) & mask), (byte) (value >>> (8 * (7 - i))));
            }
        }
        tail += Long.BYTES;

    }
    public void unsafePut(float value) {
        unsafePut(Float.floatToIntBits(value));
    }
    public void unsafePut(double value) {
        unsafePut(Double.doubleToLongBits(value));
    }
    public void unsafePut(byte[] value, int length) {
        unsafePut(value, 0, length);
    }
    public void unsafePut(byte[] value, int offset, int length) {
        int writePos = (int) (tail & mask);
        int firstPartLen = Math.min(length, capacity - writePos);
        buffer.put(writePos, value, offset, firstPartLen);
        if (length > firstPartLen) {
            buffer.put(0, value, offset + firstPartLen, length - firstPartLen);
        }
        tail += length;
    }

    public byte unsafeGetByte() {
        byte value = buffer.get((int) (head & mask));
        head += Byte.BYTES;
        return value;
    }
    public short unsafeGetShort() {
        int readPos = (int) (head & mask);
        short value;
        if (readPos == capacity - 1) {
            value = (short) (((buffer.get(readPos) & 0xFF) << 8) | (buffer.get(0) & 0xFF));
        } else {
            value = buffer.getShort(readPos);
        }
        head += Short.BYTES;
        return value;
    }
    public int unsafeGetInt() {
        int readPos = (int) (head & mask);
        int value;
        if (readPos <= capacity - Integer.BYTES) {
            value = buffer.getInt(readPos);
        } else {
            value = 0;
            for (int i = 0; i < Integer.BYTES; i++) {
                value |= (buffer.get((int) ((head + i) & mask)) & 0xFF) << (8 * (Integer.BYTES - 1 - i));
            }
        }
        head += Integer.BYTES;
        return value;
    }
    public long unsafeGetLong() {
        int readPos = (int) (head & mask);
        long value;
        if (readPos <= capacity - Long.BYTES) {
            value = buffer.getLong(readPos);
        } else {
            value = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                value |= (long) (buffer.get((int) ((head + i) & mask)) & 0xFF) << (8 * (Long.BYTES - 1 - i));
            }
        }
        head += Long.BYTES;
        return value;
    }
    public float unsafeGetFloat() {
        return Float.intBitsToFloat(unsafeGetInt());
    }
    public double unsafeGetDouble() {
        return Double.longBitsToDouble(unsafeGetLong());
    }
    public byte[] unsafeGetBytes(int length) {
        byte[] value = new byte[length];
        unsafeGetBytes(value, length);
        return value;
    }
    public void unsafeGetBytes(byte[] dst, int length) {
        unsafeGetBytes(dst, 0, length);
    }
    public void unsafeGetBytes(byte[] dst, int offset, int length) {
        int readPos = (int) (head & mask);
        int firstPartLen = Math.min(length, capacity - readPos);
        buffer.get(readPos, dst, offset, firstPartLen);
        if (length > firstPartLen) {
            buffer.get(0, dst, firstPartLen, length - firstPartLen);
        }
        head += length;
    }

    @Override
    protected void onReset() {
        clear();
    }

    @Override
    protected void onDiscard() {
        buffer.clear();
        readViews[0] = null;
        readViews[1] = null;
        writeViews[0] = null;
        writeViews[1] = null;
    }

    /**
     * 限制 ByteBuffer 数组的总可用长度
     * @param views 原始视图数组
     * @param maxBytes 最大允许的长度
     * @return 调整后的视图数组
     */
    private ByteBuffer[] applyLimit(ByteBuffer[] views, int maxBytes) {
        int remaining = maxBytes;
        for (ByteBuffer view : views) {
            int viewAvailable = view.remaining();
            if (viewAvailable > remaining) {
                view.limit(view.position() + remaining);
                remaining = 0;
            } else {
                remaining -= viewAvailable;
            }
        }
        return views;
    }

    private void throwOverflow(int required) {
        throw new LoopBufferOverflowException(required, writableBytes());
    }
    private void throwUnderflow(int requested) {
        throw new LoopBufferUnderflowException(requested, readableBytes());
    }

    /**
     * 有限可读视图
     *
     * @author tbrtz647@outlook.com
     * @since 2026/03/13
     * @version 1.0.0
     */
    public class LimitedReadableView {
        private int quota;

        private LimitedReadableView() {}

        public byte getByte() {
            if (quota < Byte.BYTES) {
                throwQuote(Byte.BYTES);
            }
            byte value = LoopBuffer.this.unsafeGetByte();
            quota -= Byte.BYTES;
            return value;
        }
        public short getShort() {
            if (quota < Short.BYTES) {
                throwQuote(Short.BYTES);
            }
            short value = LoopBuffer.this.unsafeGetShort();
            quota -= Short.BYTES;
            return value;
        }
        public int getInt() {
            if (quota < Integer.BYTES) {
                throwQuote(Integer.BYTES);
            }
            int value = LoopBuffer.this.unsafeGetInt();
            quota -= Integer.BYTES;
            return value;
        }
        public long getLong() {
            if (quota < Long.BYTES) {
                throwQuote(Long.BYTES);
            }
            long value = LoopBuffer.this.unsafeGetLong();
            quota -= Long.BYTES;
            return value;
        }
        public float getFloat() {
            if (quota < Float.BYTES) {
                throwQuote(Float.BYTES);
            }
            float value = LoopBuffer.this.unsafeGetFloat();
            quota -= Float.BYTES;
            return value;
        }
        public double getDouble() {
            if (quota < Double.BYTES) {
                throwQuote(Double.BYTES);
            }
            double value = LoopBuffer.this.unsafeGetDouble();
            quota -= Double.BYTES;
            return value;
        }
        public byte[] getBytes(int length) {
            if (quota < length) {
                throwQuote(length);
            }
            byte[] value = unsafeGetBytes(length);
            quota -= length;
            return value;
        }
        public void getBytes(byte[] dst, int offset, int length) {
            if (quota < length) {
                throwQuote(length);
            }
            unsafeGetBytes(dst, offset, length);
            quota -= length;
        }

        public int writeTo(ByteBuffer dst) {
            if (quota <= 0) {
                return 0;
            }
            ByteBuffer[] views = readableViews();
            int written = 0;
            for (ByteBuffer src : views) {
                int canWrite = Math.min(Math.min(src.remaining(), dst.remaining()), quota);
                if (canWrite <= 0) {
                    break;
                }
                src.limit(src.position() + canWrite);
                dst.put(src);
                written += canWrite;
            }
            advanceHead(written);
            this.quota -= written;
            return written;
        }
        public int writeTo(GatheringByteChannel dst) throws IOException {
            if (quota <= 0) {
                return 0;
            }
            ByteBuffer[] views = readableViews();
            int remainingQuota = quota;
            ByteBuffer v0 = views[0];
            int len0 = v0.remaining();
            int written;
            if (len0 >= remainingQuota) {
                v0.limit(v0.position() + remainingQuota);
                written = dst.write(v0);
            } else {
                remainingQuota -= len0;
                ByteBuffer v1 = views[1];
                int len1 = v1.remaining();
                if (len1 > remainingQuota) {
                    v1.limit(v1.position() + remainingQuota);
                }
                written = (int) dst.write(views);
            }
            if (written > 0) {
                advanceHead(written);
                quota -= written;
            }
            return written;
        }

        public int remaining() {
            return quota;
        }

        private void throwQuote(int required) {
            throw new LimitedViewQuotaException(quota, required);
        }
    }

    /**
     * 有限可写视图
     *
     * @author tbrtz647@outlook.com
     * @since 2026/03/13
     * @version 1.0.0
     */
    public class LimitedWritableView {
        private int quota;

        private LimitedWritableView() {}

        public void putByte(byte value) {
            if (quota < Byte.BYTES) {
                throwQuote(Byte.BYTES);
            }
            unsafePut(value);
            quota -= Byte.BYTES;
        }
        public void putShort(short value) {
            if (quota < Short.BYTES) {
                throwQuote(Short.BYTES);
            }
            unsafePut(value);
            quota -= Short.BYTES;
        }
        public void putInt(int value) {
            if (quota < Integer.BYTES) {
                throwQuote(Integer.BYTES);
            }
            unsafePut(value);
            quota -= Integer.BYTES;
        }
        public void putLong(long value) {
            if (quota < Long.BYTES) {
                throwQuote(Long.BYTES);
            }
            unsafePut(value);
            quota -= Long.BYTES;
        }
        public void putFloat(float value) {
            if (quota < Float.BYTES) {
                throwQuote(Float.BYTES);
            }
            unsafePut(value);
            quota -= Float.BYTES;
        }
        public void putDouble(double value) {
            if (quota < Double.BYTES) {
                throwQuote(Double.BYTES);
            }
            unsafePut(value);
            quota -= Double.BYTES;
        }
        public void putBytes(byte[] value) {
            if (quota < value.length) {
                throwQuote(value.length);
            }
            unsafePut(value, value.length);
            quota -= value.length;
        }
        public void putBytes(byte[] value, int offset, int length) {
            if (quota < length) {
                throwQuote(length);
            }
            unsafePut(value, offset, length);
            quota -= length;
        }

        public int readFrom(ByteBuffer src) {
            if (quota <= 0) {
                return 0;
            }
            ByteBuffer[] views = writableViews();
            int read = 0;
            for (ByteBuffer dst : views) {
                int canRead = Math.min(Math.min(dst.remaining(), src.remaining()), quota);
                if (canRead <= 0) {
                    break;
                }
                dst.limit(dst.position() + canRead);
                dst.put(src);
                read += canRead;
            }
            advanceTail(read);
            quota -= read;
            return read;
        }
        public int readFrom(ScatteringByteChannel src) throws IOException {
            if (quota <= 0) {
                return 0;
            }
            ByteBuffer[] views = writableViews();
            int remainingQuota = quota;
            ByteBuffer v0 = views[0];
            int space0 = v0.remaining();
            int read;
            if (space0 >= remainingQuota) {
                v0.limit(v0.position() + remainingQuota);
                read = src.read(v0);
            } else {
                ByteBuffer v1 = views[1];
                int v1LimitRequest = remainingQuota - space0;
                if (v1.remaining() > v1LimitRequest) {
                    v1.limit(v1.position() + v1LimitRequest);
                }
                read = (int) src.read(views);
            }
            if (read > 0) {
                advanceTail(read);
                quota -= read;
            }
            return read;
        }

        public int remaining() {
            return quota;
        }

        private void throwQuote(int required) {
            throw new LimitedViewQuotaException(quota, required);
        }
    }
}
