package com.nebilux.nexalithic.core.security;

/**
 * 安全策略
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/02/25
 */
public interface SecurityPolicy {
    long MAGIC_NUMBER = 0x494D5450;
    int MAGIC_NUMBER_LENGTH = Long.BYTES;
    int certificatesLength();
    int signatureLength();
}
