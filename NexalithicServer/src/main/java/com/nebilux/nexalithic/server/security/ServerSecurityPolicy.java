package com.nebilux.nexalithic.server.security;

import com.nebilux.nexalithic.core.security.SecurityPolicy;

import java.nio.ByteBuffer;

/**
 * 服务端安全策略
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/17
 * @version 1.0.0
 */
public interface ServerSecurityPolicy extends SecurityPolicy {
    void certificatesToBuffer(ByteBuffer output);
    void signature(byte[] input, ByteBuffer output) throws Exception;
}
