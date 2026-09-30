package com.nebilux.nexalithic.client.security;

import com.nebilux.nexalithic.core.security.SecurityPolicy;

import java.nio.ByteBuffer;

/**
 * 客户端安全策略
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/17
 * @version 1.0.0
 */
public interface ClientSecurityPolicy extends SecurityPolicy {
    byte[] rootPublicKey();
    void certificatesFormBuffer(ByteBuffer buffer);
    boolean verify(ByteBuffer buffer);
}
