package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 基于 {@link BusinessPacket} 响应包的结果转换器公共基类。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
abstract class AbstractBusinessPacketResultConverter<HC extends HandlerContext<?>> {

    /**
     * 创建响应包。
     *
     */
    protected final BusinessPacket createResponse() {
        return BusinessPacket.create(BusinessPacket.Way.RESPONSE_Ok);
    }
}
