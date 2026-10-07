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

import com.nebilux.nexalithic.core.security.SecurityPolicy;

import java.nio.ByteBuffer;

/**
 * 客户端安全策略
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface ClientSecurityPolicy extends SecurityPolicy {
    byte[] rootPublicKey();
    void certificatesFormBuffer(ByteBuffer buffer);
    boolean verify(ByteBuffer buffer);
}
