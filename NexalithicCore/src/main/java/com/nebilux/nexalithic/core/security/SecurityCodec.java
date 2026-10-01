package com.nebilux.nexalithic.core.security;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;

/**
 * 安全编解码器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SecurityCodec {
    public static final int FRAME_HEADER_LENGTH = Short.BYTES;
    public static final int MAX_FRAME_SIZE = 1024 * 16;
    public static final int MAX_PAYLOAD_SIZE = MAX_FRAME_SIZE - FRAME_HEADER_LENGTH - SecretKeyContext.TAG_LENGTH;
    private final SecretKeyContext secretKeyContext;

    public SecurityCodec(SecretKeyContext secretKeyContext) {
        this.secretKeyContext = secretKeyContext;
    }

    /** 加密 */
    public final boolean encrypt(LoopBuffer srcBuffer, LoopBuffer dstBuffer) throws InvalidAlgorithmParameterException, ShortBufferException, IllegalBlockSizeException, BadPaddingException, InvalidKeyException {
        int flag = srcBuffer.readableBytes();
        while (srcBuffer.readableBytes() > 0) {
            int payloadLength = Math.min(srcBuffer.readableBytes(), MAX_PAYLOAD_SIZE);
            int cipherLength = payloadLength + SecretKeyContext.TAG_LENGTH;
            if (dstBuffer.writableBytes() < FRAME_HEADER_LENGTH + cipherLength) {
                break;
            }
            dstBuffer.put((short) payloadLength);
            ByteBuffer[] srcs = srcBuffer.readableViews();
            ByteBuffer[] dsts = dstBuffer.writableViews();
            if (srcs[0].remaining() >= payloadLength) {
                srcs[0].limit(srcs[0].position() + payloadLength);
                if (dsts[0].remaining() >= cipherLength) {
                    dsts[0].limit(dsts[0].position() + cipherLength);
                    secretKeyContext.encrypt(srcs[0], dsts[0]);
                    dstBuffer.advanceTail(cipherLength);
                } else {
                    dstBuffer.unsafePut(secretKeyContext.encrypt(srcs[0]), cipherLength);
                }
                srcBuffer.advanceHead(payloadLength);
            } else {
                byte[] payload = new byte[payloadLength];
                srcBuffer.unsafeGetBytes(payload, payloadLength);
                dstBuffer.unsafePut(secretKeyContext.encrypt(payload), cipherLength);
            }
        }
        return flag != srcBuffer.readableBytes();
    }

    /** 解密 */
    public final boolean decrypt(LoopBuffer srcBuffer, LoopBuffer dstBuffer) throws InvalidAlgorithmParameterException, IllegalBlockSizeException, ShortBufferException, BadPaddingException, InvalidKeyException {
        int flag = srcBuffer.readableBytes();
        while (srcBuffer.readableBytes() > FRAME_HEADER_LENGTH) {
            srcBuffer.markHead();
            int payloadLength = srcBuffer.getShort();
            int cipherLength = payloadLength + SecretKeyContext.TAG_LENGTH;
            if (srcBuffer.readableBytes() < cipherLength || dstBuffer.writableBytes() < payloadLength) {
                srcBuffer.resetHead();
                break;
            }
            ByteBuffer[] srcs = srcBuffer.readableViews();
            ByteBuffer[] dsts = dstBuffer.writableViews();
            if (srcs[0].remaining() >= cipherLength) {
                srcs[0].limit(srcs[0].position() + cipherLength);
                if (dsts[0].remaining() >= payloadLength) {
                    dsts[0].limit(dsts[0].position() + payloadLength);
                    secretKeyContext.decrypt(srcs[0], dsts[0]);
                    dstBuffer.advanceTail(payloadLength);
                } else {
                    dstBuffer.unsafePut(secretKeyContext.decrypt(srcs[0]), payloadLength);
                }
                srcBuffer.advanceHead(cipherLength);
            } else {
                byte[] cipher = new byte[cipherLength];
                srcBuffer.unsafeGetBytes(cipher, cipherLength);
                dstBuffer.unsafePut(secretKeyContext.decrypt(cipher), payloadLength);
            }
        }
        return flag != srcBuffer.readableBytes();
    }
}
