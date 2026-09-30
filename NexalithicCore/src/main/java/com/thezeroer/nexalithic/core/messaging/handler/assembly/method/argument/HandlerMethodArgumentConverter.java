package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * Handler 方法参数转换器。
 *
 * <p>参数转换器负责在 Handler 执行期，把当前 {@link HandlerContext}
 * 转换为 Controller 方法中某一个形参需要的实际参数值。</p>
 *
 * <p>该接口只描述单个参数的转换策略，不负责保存多个转换器，也不负责选择转换器。
 * 转换器选择由 {@link HandlerMethodArgumentConverterSelector} 完成。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public interface HandlerMethodArgumentConverter<HC extends HandlerContext<?>> {

    /**
     * 判断当前转换器是否支持指定方法参数。
     *
     * @param method Handler 方法
     * @param parameter 目标形参
     * @param index 形参在方法参数列表中的下标
     * @return 支持该形参时返回 {@code true}
     */
    boolean supports(Method method, Parameter parameter, int index);

    /**
     * 将当前 Handler 上下文转换为目标形参的实参值。
     *
     * @param context 当前请求上下文
     * @param method Handler 方法
     * @param parameter 目标形参
     * @param index 形参在方法参数列表中的下标
     * @return 传入 Handler 方法的实参值
     * @throws Exception 转换失败时抛出
     */
    Object convert(HC context, Method method, Parameter parameter, int index) throws Exception;

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
