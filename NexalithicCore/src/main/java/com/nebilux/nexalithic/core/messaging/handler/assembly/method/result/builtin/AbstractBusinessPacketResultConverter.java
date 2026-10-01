package com.nebilux.nexalithic.core.messaging.handler.assembly.method.result.builtin;

import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 基于 {@link BusinessPacket} 响应包的结果转换器公共基类。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
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
