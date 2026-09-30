package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

import java.lang.reflect.Method;

/**
 * Handler 方法结果转换器。
 *
 * <p>结果转换器负责在 Controller 方法调用完成后，处理该方法返回的结果对象。
 * 处理动作可以是推送响应包、转换 DTO、忽略 {@code void}/{@code null} 返回值，
 * 或触发其他与当前 {@link HandlerContext} 相关的响应行为。</p>
 *
 * <p>该接口只描述单个返回结果的处理策略，不负责保存多个转换器，也不负责选择转换器。
 * 转换器选择由 {@link HandlerMethodResultConverterSelector} 完成。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public interface HandlerMethodResultConverter<HC extends HandlerContext<?>> {

    /**
     * 判断当前转换器是否支持指定 Handler 方法返回类型。
     *
     * @param method Handler 方法
     * @param resultType Handler 方法返回类型
     * @return 支持该返回类型时返回 {@code true}
     */
    boolean supports(Method method, Class<?> resultType);

    /**
     * 处理 Handler 方法返回结果。
     *
     * @param context 当前请求上下文
     * @param method Handler 方法
     * @param result Handler 方法返回值；方法返回 {@code void} 时为 {@code null}
     * @throws Exception 结果处理失败时抛出
     */
    void convert(HC context, Method method, Object result) throws Exception;

    /**
     * 返回转换器优先级。
     *
     * <p>数值越大优先级越高。选择器会优先使用高优先级转换器。</p>
     *
     * @return 转换器优先级
     */
    default int priority() {
        return 0;
    }
}
