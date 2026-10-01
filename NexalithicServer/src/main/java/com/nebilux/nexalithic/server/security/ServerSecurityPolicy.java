package com.nebilux.nexalithic.server.security;

import com.nebilux.nexalithic.core.security.SecurityPolicy;

import java.nio.ByteBuffer;

/**
 * 服务端安全策略
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface ServerSecurityPolicy extends SecurityPolicy {
    void certificatesToBuffer(ByteBuffer output);
    void signature(byte[] input, ByteBuffer output) throws Exception;
}
