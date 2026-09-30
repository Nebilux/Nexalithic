package com.nebilux.nexalithic.client.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 空客户端安全策略
 * <p><b>警告：</b> 该策略会默认跳过所有服务端证书校验并返回验证成功。
 * 这将使客户端完全暴露在伪造服务器的风险下。请勿在正式环境中使用。</p>
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/12
 * @version 1.0.0
 */
public final class EmptyClientSecurityPolicy implements ClientSecurityPolicy {
    private static final Logger logger = LoggerFactory.getLogger(EmptyClientSecurityPolicy.class);
    private static final EmptyClientSecurityPolicy INSTANCE = new EmptyClientSecurityPolicy();

    private EmptyClientSecurityPolicy() {}

    public static EmptyClientSecurityPolicy INSTANCE() {
        logger.warn("SECURITY ALERT: EmptyClientSecurityPolicy. Server identity verification is COMPLETELY DISABLED!");
        return INSTANCE;
    }

    @Override
    public int certificatesLength() {
        return 0;
    }

    @Override
    public int signatureLength() {
        return 0;
    }

    @Override
    public byte[] rootPublicKey() {
        return null;
    }

    @Override
    public void certificatesFormBuffer(ByteBuffer buffer) {

    }

    @Override
    public boolean verify(ByteBuffer buffer) {
        return true;
    }
}
