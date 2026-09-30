package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverter;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.annotation.RequestWay;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 将当前请求包的 {@link BusinessPacket.Way} 解析为 Handler 方法参数。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class BusinessPacketWayArgumentConverter<HC extends HandlerContext<?>> extends AbstractBusinessPacketArgumentConverter<HC> implements HandlerMethodArgumentConverter<HC> {

    @Override
    public boolean supports(Method method, Parameter parameter, int index) {
        return parameter.isAnnotationPresent(RequestWay.class) && parameter.getType() == BusinessPacket.Way.class;
    }

    @Override
    public Object convert(HC context, Method method, Parameter parameter, int index) {
        return request(context, method).getWay();
    }

    @Override
    public int priority() {
        return 900;
    }
}
