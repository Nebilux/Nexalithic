package com.thezeroer.nexalithic.core.messaging.handler;

/**
 * Handler 的实际业务调用函数。
 *
 * <p>该函数式接口是 {@link NexalithicHandler} 与具体业务方法之间的适配层。
 * 控制器装配器会把符合约束的 Controller 方法转换为该接口实例，
 * Handler 执行时只需要向其传入当前 {@link HandlerContext}。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/16
 * @version 1.0.0
 */
@FunctionalInterface
public interface HandlerInvoker<HC extends HandlerContext<?>> {
    /**
     * 执行业务处理逻辑。
     *
     * @param context 当前请求上下文
     */
    void invoke(HC context);
}
