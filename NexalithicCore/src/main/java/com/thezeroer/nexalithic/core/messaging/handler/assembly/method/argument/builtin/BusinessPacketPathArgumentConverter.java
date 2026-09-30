package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverter;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.annotation.RequestPath;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 将当前请求包路径解析为防御性复制后的 {@code short[]} 参数。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class BusinessPacketPathArgumentConverter<HC extends HandlerContext<?>> extends AbstractBusinessPacketArgumentConverter<HC> implements HandlerMethodArgumentConverter<HC> {

    @Override
    public boolean supports(Method method, Parameter parameter, int index) {
        return parameter.isAnnotationPresent(RequestPath.class) && parameter.getType() == short[].class;
    }

    @Override
    public Object convert(HC context, Method method, Parameter parameter, int index) {
        short[] path = request(context, method).getPath();
        return path == null ? null : path.clone();
    }

    @Override
    public int priority() {
        return 900;
    }
}
