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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 空服务器安全策略
 * <p><b>警告：</b> 该策略不提供任何身份、签名验证。仅建议在开发调试环境或
 * 物理隔离的受信网络中使用。在公网环境使用会导致系统极易受到中间人 (MITM) 攻击。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class EmptyServerSecurityPolicy implements ServerSecurityPolicy {
    private static final Logger logger = LoggerFactory.getLogger(EmptyServerSecurityPolicy.class);
    private static final EmptyServerSecurityPolicy INSTANCE = new EmptyServerSecurityPolicy();
    private EmptyServerSecurityPolicy() {}

    public static EmptyServerSecurityPolicy INSTANCE() {
        logger.warn("SECURITY ALERT: EmptyServerSecurityPolicy. Communication will NOT be signed or authenticated!");
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
    public void certificatesToBuffer(ByteBuffer output) {

    }

    @Override
    public void signature(byte[] input, ByteBuffer output) {

    }
}
