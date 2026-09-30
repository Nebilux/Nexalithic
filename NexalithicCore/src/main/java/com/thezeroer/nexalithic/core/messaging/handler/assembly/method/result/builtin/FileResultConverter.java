package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverter;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.model.packet.business.payload.FilePayload;

import java.io.File;
import java.lang.reflect.Method;

/**
 * 将 Handler 方法返回的 {@link File} 包装为 {@link FilePayload} 响应。
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class FileResultConverter<HC extends HandlerContext<?>> extends AbstractBusinessPacketResultConverter<HC> implements HandlerMethodResultConverter<HC> {

    @Override
    public boolean supports(Method method, Class<?> resultType) {
        return File.class.isAssignableFrom(resultType);
    }

    @Override
    public void convert(HC context, Method method, Object result) throws Exception {
        BusinessPacket response = createResponse();
        if (result != null) {
            response.attach(new FilePayload((File) result));
        }
        context.pushResponse(response);
    }

    @Override
    public int priority() {
        return 800;
    }
}
