package com.nebilux.nexalithic.core.model.packet.business.payload;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 文本有效载荷
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class TextPayload extends AbstractPayload<String> {
    public static final long UID = 1;
    private static final byte TYPE_NULL = 0x00;
    private static final byte TYPE_EMPTY = 0x01;
    private static final byte TYPE_NORMAL = 0x02;

    private byte[] bytes;
    private byte type;
    private boolean metaProcessed;

    public TextPayload() {}
    public TextPayload(String text) {
        value = text;
        if (text == null) {
            type = TYPE_NULL;
        } else if (text.isEmpty()) {
            type = TYPE_EMPTY;
        } else {
            type = TYPE_NORMAL;
            bytes = text.getBytes(StandardCharsets.UTF_8);
            totalSize = bytes.length;
        }
        totalSize += Byte.BYTES;
        if (totalSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Payload too large, maximum size is " + Integer.MAX_VALUE);
        }
    }

    @Override
    public int encode(LoopBuffer.LimitedWritableView output) {
        int remaining = (int) Math.min(output.remaining(), totalSize - processedSize);
        if (remaining <= 0) {
            return 0;
        }
        int offset = (int) processedSize, length = 0;
        if (!metaProcessed) {
            output.putByte(type);
            metaProcessed = true;
            remaining -= Byte.BYTES;
            length = Byte.BYTES;
        } else {
            offset -= Byte.BYTES;
        }
        length += remaining;
        if (type == TYPE_NORMAL) {
            output.putBytes(bytes, offset, remaining);
        }
        processedSize += length;
        return length;
    }
    @Override
    public int decode(LoopBuffer.LimitedReadableView input) {
        int remaining = (int) Math.min(input.remaining(), totalSize - processedSize);
        if (remaining <= 0) {
            return 0;
        }
        int offset = (int) processedSize, length = 0;
        if (!metaProcessed) {
            type = input.getByte();
            metaProcessed = true;
            remaining -= Byte.BYTES;
            length = Byte.BYTES;
        } else {
            offset -= Byte.BYTES;
        }
        length += remaining;
        if (type == TYPE_NORMAL) {
            input.getBytes(bytes, offset, remaining);
        }
        processedSize += length;
        return length;
    }

    @Override
    public AbstractPayload<String> duplicate() {
        TextPayload clone = new TextPayload();
        clone.value = this.value;
        clone.bytes = this.bytes;
        clone.type = this.type;
        clone.metaProcessed = this.metaProcessed;
        clone.totalSize = this.totalSize;
        clone.processedSize = 0;
        return clone;
    }

    @Override
    public void prepareDecode(long totalSize) throws IOException {
        super.prepareDecode(totalSize);
        bytes = new byte[(int) (totalSize - Byte.BYTES)];
    }
    @Override
    public void finishDecode() {
        switch (type) {
            case TYPE_EMPTY -> value = "";
            case TYPE_NORMAL -> value = new String(bytes, StandardCharsets.UTF_8);
        }
    }

    @Override
    public long getPayloadUID() {
        return UID;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (value == null ? ": null" : ": \"" + value + "\"");
    }
}
