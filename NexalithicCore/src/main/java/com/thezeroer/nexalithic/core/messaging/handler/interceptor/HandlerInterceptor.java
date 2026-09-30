package com.thezeroer.nexalithic.core.messaging.handler.interceptor;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerMetadata;
import com.thezeroer.nexalithic.core.messaging.handler.NexalithicHandler;

/**
 * Handler 拦截器。
 *
 * <p>拦截器围绕 {@link NexalithicHandler}
 * 的业务调用执行，可用于鉴权、限流、审计、指标采集或上下文增强等横切逻辑。</p>
 *
 * <p>默认实现全部为空操作，其中 {@link #onBefore(HandlerContext, HandlerMetadata)}
 * 默认允许 Handler 继续执行。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/06/13
 */
public interface HandlerInterceptor<HC extends HandlerContext<?>> {
    /**
     * Handler 执行前调用。
     *
     * @param context 当前请求上下文
     * @param metadata 当前 Handler 元数据
     * @return true 表示继续执行；false 表示拦截本次请求
     * @throws Exception 前置处理失败时抛出
     */
    default boolean onBefore(HC context, HandlerMetadata metadata) throws Exception {
        return true;
    }

    /**
     * Handler 正常执行后调用。
     *
     * 只有 Handler 正常执行完成时才会调用。
     *
     * @param context 当前请求上下文
     * @param metadata 当前 Handler 元数据
     * @throws Exception 后置处理失败时抛出
     */
    default void onAfter(HC context, HandlerMetadata metadata) throws Exception {
    }

    /**
     * 本次 Handler 调用流程结束时调用。
     *
     * 无论 Handler 成功、异常、异步异常，都会在最终阶段调用。
     * 如果没有异常，{@code exception} 为 null。
     *
     * @param context 当前请求上下文
     * @param metadata 当前 Handler 元数据
     * @param exception 本次调用中的异常；正常完成时为 {@code null}
     */
    default void onCompletion(HC context, HandlerMetadata metadata, Exception exception) {
    }
}
