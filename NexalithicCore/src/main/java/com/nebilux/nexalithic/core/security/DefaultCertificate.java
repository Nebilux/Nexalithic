package com.nebilux.nexalithic.core.security;

import java.nio.ByteBuffer;

/**
 * 默认证书
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/12
 * @version 1.0.0
 */
public record DefaultCertificate(int version, long creationTime, long expirationTime, byte[] publicKey, byte[] signature) implements NexalithicCertificate {

    @Override
    public int publicKeyLength() {
        if (publicKey == null) {
            return 0;
        }
        return publicKey.length;
    }

    @Override
    public int signatureLength() {
        if (signature == null) {
            return 0;
        }
        return signature.length;
    }

    @Override
    public void toBuffer(ByteBuffer buffer) {
        buffer.putInt(version()).putLong(creationTime()).putLong(expirationTime()).putInt(publicKeyLength()).putInt(signatureLength()).put(publicKey()).put(signature());
    }

    @Override
    public byte[] getRawData() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES * 2 +publicKeyLength());
        buffer.putInt(version()).putLong(creationTime()).putLong(expirationTime()).put(publicKey());
        return buffer.array();
    }

    public static NexalithicCertificate fromBuffer(ByteBuffer buffer) {
        int version = buffer.getInt();
        long creationTime = buffer.getLong();
        long expirationTime = buffer.getLong();
        int publicKeyLength = buffer.getInt();
        int signatureLength = buffer.getInt();
        byte[] publicKey = new byte[publicKeyLength];
        buffer.get(publicKey);
        byte[] signature = new byte[signatureLength];
        buffer.get(signature);
        return new DefaultCertificate(version, creationTime, expirationTime, publicKey, signature);
    }
}
