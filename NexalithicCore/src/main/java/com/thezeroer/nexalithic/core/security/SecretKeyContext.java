package com.thezeroer.nexalithic.core.security;

import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 密钥上下文
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/05
 * @version 1.0.0
 */
public class SecretKeyContext {
    public static final String AES_ALGORITHM = "AES/GCM/NoPadding";
    public static final int NONCE_LENGTH = 12;
    public static final int TAG_LENGTH = 16;
    private final SecretKey readKey, writeKey;
    private final byte[] readIV, writeIV;
    private final AtomicInteger readSequence = new AtomicInteger(0);
    private final AtomicInteger writeSequence = new AtomicInteger(0);
    private final Cipher readCipher = Cipher.getInstance(AES_ALGORITHM);
    private final Cipher writeCipher = Cipher.getInstance(AES_ALGORITHM);

    public SecretKeyContext(SecretKey readKey, byte[] readIV, SecretKey writeKey, byte[] writeIV) throws NoSuchPaddingException, NoSuchAlgorithmException {
        this.readKey = readKey;
        this.readIV = readIV;
        this.writeKey = writeKey;
        this.writeIV = writeIV;
    }

    public byte[] nextReadNonce() {
        byte[] nonce = new byte[NONCE_LENGTH];
        System.arraycopy(readIV, 0, nonce, 0, NONCE_LENGTH);
        int seq = readSequence.getAndIncrement();
        nonce[8]  ^= (byte) (seq >>> 24);
        nonce[9]  ^= (byte) (seq >>> 16);
        nonce[10] ^= (byte) (seq >>> 8);
        nonce[11] ^= (byte) seq;
        return nonce;
    }
    public byte[] nextWriteNonce() {
        byte[] nonce = new byte[NONCE_LENGTH];
        System.arraycopy(writeIV, 0, nonce, 0, NONCE_LENGTH);
        int seq = writeSequence.getAndIncrement();
        nonce[8]  ^= (byte) (seq >>> 24);
        nonce[9]  ^= (byte) (seq >>> 16);
        nonce[10] ^= (byte) (seq >>> 8);
        nonce[11] ^= (byte) seq;
        return nonce;
    }

    /**
     * 加密
     */
    public void encrypt(ByteBuffer input, ByteBuffer output) throws InvalidKeyException, InvalidAlgorithmParameterException, ShortBufferException, IllegalBlockSizeException, BadPaddingException {
        writeCipher.init(Cipher.ENCRYPT_MODE, writeKey, new GCMParameterSpec(TAG_LENGTH * Byte.SIZE, nextWriteNonce()));
        writeCipher.doFinal(input, output);
    }

    /**
     * 解密
     */
    public void decrypt(ByteBuffer input, ByteBuffer output) throws InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, ShortBufferException {
        readCipher.init(Cipher.DECRYPT_MODE, readKey, new GCMParameterSpec(TAG_LENGTH * Byte.SIZE, nextReadNonce()));
        readCipher.doFinal(input, output);
    }

    public byte[] encrypt(byte[] input) throws InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException {
        writeCipher.init(Cipher.ENCRYPT_MODE, writeKey, new GCMParameterSpec(TAG_LENGTH * Byte.SIZE, nextWriteNonce()));
        return writeCipher.doFinal(input);
    }
    public byte[] decrypt(byte[] input) throws InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException {
        readCipher.init(Cipher.DECRYPT_MODE, readKey, new GCMParameterSpec(TAG_LENGTH * Byte.SIZE, nextReadNonce()));
        return readCipher.doFinal(input);
    }

    public byte[] encrypt(ByteBuffer input) throws InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        writeCipher.init(Cipher.ENCRYPT_MODE, writeKey, new GCMParameterSpec(TAG_LENGTH * Byte.SIZE, nextWriteNonce()));
        if (input.hasArray()) {
            int offset = input.arrayOffset() + input.position();
            byte[] result = writeCipher.doFinal(input.array(), offset, input.remaining());
            input.position(input.limit());
            return result;
        } else {
            byte[] bytes = new byte[input.remaining()];
            input.get(bytes);
            return writeCipher.doFinal(bytes);
        }
    }
    public byte[] decrypt(ByteBuffer input) throws InvalidAlgorithmParameterException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        readCipher.init(Cipher.DECRYPT_MODE, readKey, new GCMParameterSpec(TAG_LENGTH * Byte.SIZE, nextReadNonce()));
        if (input.hasArray()) {
            int offset = input.arrayOffset() + input.position();
            byte[] result = readCipher.doFinal(input.array(), offset, input.remaining());
            input.position(input.limit());
            return result;
        } else {
            byte[] bytes = new byte[input.remaining()];
            input.get(bytes);
            return readCipher.doFinal(bytes);
        }
    }
}
