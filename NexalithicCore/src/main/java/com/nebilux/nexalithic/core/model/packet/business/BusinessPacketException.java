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

package com.nebilux.nexalithic.core.model.packet.business;

import com.nebilux.nexalithic.core.exception.NexalithicFailure;
import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 业务包或其载荷使用方式不符合约束的异常。
 *
 * <p>例如路径深度超限、向已封存的包附加载荷、复制未封存的广播包，
 * 或文本载荷过大、文件载荷复制失败。载荷数量超限由其子类表示。</p>
 */
public class BusinessPacketException extends NexalithicUncheckedException {
    protected BusinessPacketException(String message) {
        super(message);
    }

    private BusinessPacketException(String message, Throwable cause) {
        super(message, cause);
    }

    public static BusinessPacketException pathDepthExceeded(int maximum) {
        return new BusinessPacketException("path length exceeds maximum of " + maximum);
    }

    public static BusinessPacketException attachmentToSealedPacket() {
        return new BusinessPacketException("Cannot attach payload to a sealed packet.");
    }

    public static BusinessPacketException broadcastDuplicationRequiresSealedPacket() {
        return new BusinessPacketException("Only sealed packets can be duplicated for broadcast.");
    }

    public static BusinessPacketException payloadTooLarge(int maximum) {
        return new BusinessPacketException("Payload too large, maximum size is " + maximum);
    }

    public static BusinessPacketException failedToDuplicate(Class<?> target, Throwable cause) {
        return new BusinessPacketException(NexalithicFailure.failedTo("duplicate", target.getSimpleName()), cause);
    }
}
