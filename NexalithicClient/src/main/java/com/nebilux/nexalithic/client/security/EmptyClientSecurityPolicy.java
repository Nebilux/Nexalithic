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

package com.nebilux.nexalithic.client.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 空客户端安全策略
 * <p><b>警告：</b> 该策略会默认跳过所有服务端证书校验并返回验证成功。
 * 这将使客户端完全暴露在伪造服务器的风险下。请勿在正式环境中使用。</p>
 *
 * @author Reonvia
 * @since 0.1.0
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
