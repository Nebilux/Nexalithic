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

package com.nebilux.nexalithic.server.security;

import com.nebilux.nexalithic.core.security.NexalithicCertificate;

import java.nio.ByteBuffer;
import java.security.*;

/**
 * 默认服务器安全策略
 *
 * @author Reonvia
 * @since 0.1.0
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
