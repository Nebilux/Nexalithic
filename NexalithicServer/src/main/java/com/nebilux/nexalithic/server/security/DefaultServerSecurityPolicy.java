package com.nebilux.nexalithic.server.security;

import com.nebilux.nexalithic.core.security.NexalithicCertificate;

import java.nio.ByteBuffer;
import java.security.*;

/**
 * 默认服务器安全策略
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/26
 * @version 1.0.0
 */
public abstract class DefaultServerSecurityPolicy implements ServerSecurityPolicy {
    public static final int SIGNATURE_LENGTH = 64;
    public static final String SIGNATURE_ALGORITHM = "Ed25519";
    private static final ThreadLocal<Signature> SIGNATURE_CACHE = ThreadLocal.withInitial(() -> {
        try {
            return Signature.getInstance(SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    });
    private final ByteBuffer certificatesBuffer;

    protected DefaultServerSecurityPolicy() throws NoSuchAlgorithmException {
        Signature.getInstance(SIGNATURE_ALGORITHM);
        NexalithicCertificate issuingCertificate = getIssuingCertificate();
        NexalithicCertificate leafCertificate = getLeafCertificate();
        certificatesBuffer = ByteBuffer.allocate(certificatesLength());
        leafCertificate.toBuffer(certificatesBuffer);
        issuingCertificate.toBuffer(certificatesBuffer);
    }

    public int signatureLength() {
        return SIGNATURE_LENGTH;
    }

    public void certificatesToBuffer(ByteBuffer output) {
        output.put(certificatesBuffer.flip());
    }

    public void signature(byte[] input, ByteBuffer output) throws InvalidKeyException, SignatureException {
        Signature signature = SIGNATURE_CACHE.get();
        signature.initSign(getLeafCertificatePrivateKey());
        signature.update(input);
        output.put(signature.sign());
    }

    protected abstract NexalithicCertificate getIssuingCertificate();
    protected abstract NexalithicCertificate getLeafCertificate();
    protected abstract PrivateKey getLeafCertificatePrivateKey();
}
