package com.nebilux.nexalithic.core.security;

import java.nio.ByteBuffer;

/**
 * Nexalithic 证书
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface NexalithicCertificate {
    int BASE_LENGTH = Integer.BYTES * 3 + Long.BYTES * 2;
    int version();
    long creationTime();
    long expirationTime();
    int publicKeyLength();
    int signatureLength();
    byte[] publicKey();
    byte[] signature();

    default int totalSize() {
        return BASE_LENGTH + publicKeyLength() + signatureLength();
    }
    void toBuffer(ByteBuffer buffer);
    byte[] getRawData();
}
