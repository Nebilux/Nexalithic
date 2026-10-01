package com.nebilux.nexalithic.client.messaging;

import com.nebilux.nexalithic.client.session.ClientSession;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;

/**
 * 客户端处理器上下文
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClientHandlerContext extends HandlerContext<ClientSession> {

    public ClientHandlerContext() {}

    public static class Recyclable extends HandlerContext.Recyclable<
            ClientSession,
            ClientHandlerContext,
            Recyclable
        > {
        public Recyclable(GenericWrapperPool<ClientHandlerContext, Recyclable> owner, ClientHandlerContext target) {
            super(owner, target);
        }
    }
}
