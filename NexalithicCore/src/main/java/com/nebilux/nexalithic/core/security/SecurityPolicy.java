package com.nebilux.nexalithic.core.security;

/**
 * 安全策略
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface SecurityPolicy {
    long MAGIC_NUMBER = 0x494D5450;
    int MAGIC_NUMBER_LENGTH = Long.BYTES;
    int certificatesLength();
    int signatureLength();
}
