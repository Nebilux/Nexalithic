package com.nebilux.nexalithic.client.security;

import com.nebilux.nexalithic.core.security.DefaultCertificate;
import com.nebilux.nexalithic.core.security.NexalithicCertificate;
import com.nebilux.nexalithic.core.security.SecretKeyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

/**
 * 默认客户端安全策略
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/26
 * @version 1.0.0
 */
public abstract class DefaultClientSecurityPolicy implements ClientSecurityPolicy {
    private static final Logger logger = LoggerFactory.getLogger(DefaultClientSecurityPolicy.class);
    public static final int SIGNATURE_LENGTH = 64;
    public static final String SIGNATURE_ALGORITHM = "Ed25519";

    protected NexalithicCertificate remoteIssuingCertificate;
    protected NexalithicCertificate remoteLeafCertificate;
    protected NexalithicCertificate localIssuingCertificate;
    protected NexalithicCertificate localLeafCertificate;

    public int signatureLength() {
        return SIGNATURE_LENGTH;
    }

    @Override
    public void certificatesFormBuffer(ByteBuffer buffer) {
        remoteLeafCertificate = DefaultCertificate.fromBuffer(buffer);
        remoteIssuingCertificate = DefaultCertificate.fromBuffer(buffer);
    }

    @Override
    public boolean verify(ByteBuffer buffer) {
        try {
            loadLocalCertificates();
            if (!verifyCertificates()) {
                return false;
            }
            byte[] data = new byte[SecretKeyUtils.ECDH_LENGTH];
            byte[] signature = new byte[SIGNATURE_LENGTH];
            buffer.get(data).get(signature);
            if (!verifyDataSignature(data, signature, remoteLeafCertificate.publicKey())) {
                return false;
            }
            saveRemoteCertificates();
            return true;
        } catch (Exception e) {
            logger.error("Security verification failed", e);
            return false;
        }
    }

    protected abstract void loadLocalCertificates();
    protected abstract void saveRemoteCertificates();

    private boolean verifyCertificates() throws NoSuchAlgorithmException, InvalidKeySpecException, SignatureException, InvalidKeyException {
        if (remoteLeafCertificate == null || remoteIssuingCertificate == null) {
            logger.error("Certificates are not initialized");
            return false;
        }
        long now = System.currentTimeMillis();
        if (isExpired(remoteLeafCertificate, now) || isExpired(remoteIssuingCertificate, now)) {
            logger.warn("Certificates has expired members");
            return false;
        }
        if (!verifyCertSignature(remoteIssuingCertificate, rootPublicKey())) {
            logger.warn("Issuing certificate verification failed (Root -> Issuing)");
            return false;
        }
        if (!verifyCertSignature(remoteLeafCertificate, remoteIssuingCertificate.publicKey())) {
            logger.warn("Leaf certificate verification failed (Issuing -> Leaf)");
            return false;
        }
        if (localIssuingCertificate != null && remoteIssuingCertificate.version() < localIssuingCertificate.version()) {
            logger.warn("Certificate rollback detected!");
            return false;
        }
        if (localLeafCertificate != null && remoteLeafCertificate.version() < localLeafCertificate.version()) {
            logger.warn("Certificate rollback detected!");
            return false;
        }
        return true;
    }
    private boolean isExpired(NexalithicCertificate cert, long now) {
        return now < cert.creationTime() || now > cert.expirationTime();
    }
    private boolean verifyCertSignature(NexalithicCertificate cert, byte[] publicKeyBytes) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
        return verifyDataSignature(cert.getRawData(), cert.signature(), publicKeyBytes);
    }
    private boolean verifyDataSignature(byte[] rawData, byte[] signature, byte[] publicKeyBytes) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
        Signature verifier = Signature.getInstance(SIGNATURE_ALGORITHM);
        verifier.initVerify(KeyFactory.getInstance(SIGNATURE_ALGORITHM).generatePublic(new X509EncodedKeySpec((publicKeyBytes))));
        verifier.update(rawData);
        return verifier.verify(signature);
    }
}
