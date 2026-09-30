package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverter;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverterSelector;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * 默认 Handler 方法参数转换器选择器。
 *
 * <p>该实现保存一组 {@link HandlerMethodArgumentConverter}，并在构造阶段按
 * {@link HandlerMethodArgumentConverter#priority()} 从高到低排序。
 * 选择时返回第一个支持目标形参的转换器。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class DefaultHandlerMethodArgumentConverterSelector<HC extends HandlerContext<?>> implements HandlerMethodArgumentConverterSelector<HC> {
    private final List<HandlerMethodArgumentConverter<HC>> converters;

    /**
     * 创建默认参数转换器选择器。
     *
     * @param converters 可用参数转换器集合
     */
    public DefaultHandlerMethodArgumentConverterSelector(Collection<? extends HandlerMethodArgumentConverter<HC>> converters) {
        List<HandlerMethodArgumentConverter<HC>> sorted = new ArrayList<>(converters);
        sorted.sort(Comparator.comparingInt(HandlerMethodArgumentConverter<HC>::priority).reversed());
        this.converters = List.copyOf(sorted);
    }

    /**
     * 创建默认参数转换器选择器构建器。
     *
     * @param <HC> Handler 上下文类型
     * @return 构建器
     */
    public static <HC extends HandlerContext<?>> Builder<HC> builder() {
        return new Builder<>();
    }

    /**
     * 为指定 Handler 方法形参选择转换器。
     *
     * @param method Handler 方法
     * @param parameter 目标形参
     * @param index 形参在方法参数列表中的下标
     * @return 支持该形参的转换器
     * @throws IllegalStateException 没有转换器支持该形参时抛出
     */
    @Override
    public HandlerMethodArgumentConverter<HC> select(Method method, Parameter parameter, int index) {
        for (HandlerMethodArgumentConverter<HC> converter : converters) {
            if (converter.supports(method, parameter, index)) {
                return converter;
            }
        }
        throw new IllegalStateException(
                "No HandlerMethodArgumentConverter supports parameter #%d '%s' of method %s"
                        .formatted(index, parameter.getType().getName(), method.toGenericString())
        );
    }

    /**
     * {@link DefaultHandlerMethodArgumentConverterSelector} 构建器。
     *
     * @param <HC> Handler 上下文类型
     */
    public static class Builder<HC extends HandlerContext<?>> {
        private final List<HandlerMethodArgumentConverter<HC>> converters = new ArrayList<>();

        private Builder() {
        }

        /**
         * 追加一个参数转换器。
         *
         * @param converter 参数转换器
         * @return 当前构建器
         */
        public Builder<HC> converter(HandlerMethodArgumentConverter<HC> converter) {
            converters.add(converter);
            return this;
        }

        /**
         * 批量追加参数转换器。
         *
         * @param converters 参数转换器集合
         * @return 当前构建器
         */
        public Builder<HC> converters(Collection<? extends HandlerMethodArgumentConverter<HC>> converters) {
            this.converters.addAll(converters);
            return this;
        }

        /**
         * 构建默认参数转换器选择器。
         *
         * @return 默认参数转换器选择器
         */
        public DefaultHandlerMethodArgumentConverterSelector<HC> build() {
            return new DefaultHandlerMethodArgumentConverterSelector<>(converters);
        }
    }
}
