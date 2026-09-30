package com.thezeroer.nexalithic.core.messaging.handler.interceptor;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerMetadata;

import java.util.List;

/**
 * 基于数组快照的 Handler 拦截器管线。
 *
 * <p>构造时会把拦截器列表复制为数组，避免执行过程中受外部列表修改影响。
 * 前置回调按数组正序执行，后置和完成回调按数组逆序执行。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/09
 */
class ArrayInterceptorPipeline<HC extends HandlerContext<?>> implements InterceptorPipeline <HC>{
    private final HandlerMetadata metadata;
    private final HandlerInterceptor<HC>[] interceptors;

    /**
     * 创建数组拦截器管线。
     *
     * @param metadata Handler 元数据
     * @param interceptors 拦截器列表
     */
    @SuppressWarnings("unchecked")
    public ArrayInterceptorPipeline(HandlerMetadata metadata, List<HandlerInterceptor<HC>> interceptors) {
        this.metadata = metadata;
        this.interceptors = (HandlerInterceptor<HC>[]) interceptors.toArray(HandlerInterceptor[]::new);
    }

    @Override
    public boolean applyBefore(HC context) throws Exception {
        int i = 0;
        try {
            while (i < interceptors.length) {
                if (!interceptors[i].onBefore(context, metadata)) {
                    complete(context, null, i);
                    return false;
                }
                i++;
            }
            return true;
        } catch (Exception e) {
            complete(context, e, i);
            throw e;
        }
    }

    @Override
    public void applyAfter(HC context) throws Exception {
        for (int i = interceptors.length - 1; i >= 0; i--) {
            interceptors[i].onAfter(context, metadata);
        }
    }

    @Override
    public void complete(HC context, Exception exception) {
        complete(context, exception, interceptors.length - 1);
    }

    private void complete(HC context, Exception exception, int lastPassedIndex) {
        for (; lastPassedIndex >= 0; lastPassedIndex--) {
            interceptors[lastPassedIndex].onCompletion(context, metadata, exception);
        }
    }
}
