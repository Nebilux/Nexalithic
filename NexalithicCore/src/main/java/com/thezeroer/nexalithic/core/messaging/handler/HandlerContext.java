package com.thezeroer.nexalithic.core.messaging.handler;

import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.TargetStaticRecyclableWrapper;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.session.NexalithicSession;

/**
 * Handler 调用上下文。
 *
 * <p>上下文对象承载一次业务包分发所需的运行时状态，包括当前请求包、
 * 所属会话以及向对端推送响应的能力。具体客户端或服务端模块应继承该类型，
 * 补充与自身会话、连接或业务协议相关的便捷访问方法。</p>
 *
 * <p>上下文实例通常由对象池复用。复用时由 {@link Recyclable} 写入请求、
 * 会话和目标 Handler，并在回收阶段清理这些引用。</p>
 *
 * @param <S> 当前上下文绑定的会话类型
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/16
 * @version 1.0.0
 */
public abstract class HandlerContext<S extends NexalithicSession<?>> {
    /** 当前请求所属的会话。 */
    protected volatile S session;

    /** 当前 Handler 正在处理的业务请求包。 */
    protected volatile BusinessPacket request;

    /**
     * 返回当前正在处理的业务请求包。
     *
     * @return 当前请求包；上下文未初始化或已回收时可能为 {@code null}
     */
    public final BusinessPacket getRequest() {
        return request;
    }

    /**
     * 向当前上下文关联的会话推送业务响应。
     *
     * @param response 需要发送给对端的响应包
     * @return 响应成功进入发送流程时返回 {@code true}
     */
    public boolean pushResponse(BusinessPacket response) {
        return session.pushBusinessPacket(response.setTaskId(request.getTaskId()));
    }

    public static class Recyclable<S extends NexalithicSession<?>, T extends HandlerContext<S>, W extends Recyclable<S, T, W>> extends TargetStaticRecyclableWrapper<T, W> {
        private volatile NexalithicHandler<T> handler;
        public Recyclable(GenericWrapperPool<T, W> owner, T target) {
            super(owner, target);
        }

        public void initTarget(BusinessPacket request, S session, NexalithicHandler<T> handler) {
            target.request = request;
            target.session = session;
            this.handler = handler;
        }

        @Override
        protected void onReset() {
            target.request = null;
            target.session = null;
            this.handler = null;
        }

        public S getSession() {
            return target.session;
        }
        public NexalithicHandler<T> getHandler() {
            return handler;
        }
    }
}
