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

package com.nebilux.nexalithic.core.messaging.handler;

import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.TargetStaticRecyclableWrapper;
import com.nebilux.nexalithic.core.model.business.BusinessModel;

/**
 * Handler 调用上下文。
 *
 * <p>上下文对象承载一次业务模型分发所需的运行时状态，包括当前请求模型、
 * 所属会话以及向对端推送响应的能力。具体客户端或服务端模块应继承该类型，
 * 补充与自身会话、连接或业务协议相关的便捷访问方法。</p>
 *
 * <p>上下文实例通常由对象池复用。复用时由 {@link Recyclable} 写入请求、
 * 会话和目标 Handler，并在回收阶段清理这些引用。</p>
 *
 * @param <S> 当前上下文绑定的会话类型
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class HandlerContext<S extends AbstractSession<?>> {
    /** 当前请求所属的会话。 */
    protected volatile S session;

    /** 当前 Handler 正在处理的业务请求模型。 */
    protected volatile BusinessModel<?> request;

    /**
     * 返回当前业务请求模型。调用方指定的类型必须与实际请求类型一致。
     *
     * @param <M> 请求模型类型
     * @return 当前请求；上下文未初始化或已回收时返回 {@code null}
     */
    @SuppressWarnings("unchecked")
    public final <M extends BusinessModel<?>> M getRequest() {
        return (M) request;
    }

    /**
     * 向当前上下文关联的会话推送业务响应模型。
     *
     * @param response 需要发送给对端的响应模型
     * @return 响应成功进入发送流程时返回 {@code true}
     */
    public boolean pushResponse(BusinessModel<?> response) {
        if (!response.getWay().isResponse()) {
            throw new IllegalArgumentException("A response business model is required");
        }
        return session.pushBusiness(response.setTaskId(request.getTaskId()));
    }

    public static class Recyclable<S extends AbstractSession<?>, T extends HandlerContext<S>, W extends Recyclable<S, T, W>> extends TargetStaticRecyclableWrapper<T, W> {
        private volatile NexalithicHandler<T> handler;
        public Recyclable(GenericWrapperPool<T, W> owner, T target) {
            super(owner, target);
        }

        public void initTarget(BusinessModel<?> request, S session, NexalithicHandler<T> handler) {
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
