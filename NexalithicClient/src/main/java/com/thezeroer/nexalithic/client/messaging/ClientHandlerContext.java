package com.thezeroer.nexalithic.client.messaging;

import com.thezeroer.nexalithic.client.session.ClientSession;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

/**
 * 客户端处理器上下文
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/18
 * @version 1.0.0
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
