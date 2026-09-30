package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverter;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverterSelector;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * 默认 Handler 方法结果转换器选择器。
 *
 * <p>该实现保存一组 {@link HandlerMethodResultConverter}，并在构造阶段按
 * {@link HandlerMethodResultConverter#priority()} 从高到低排序。
 * 选择时返回第一个支持目标返回类型的转换器。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
public class DefaultHandlerMethodResultConverterSelector<HC extends HandlerContext<?>> implements HandlerMethodResultConverterSelector<HC> {
    private final List<HandlerMethodResultConverter<HC>> converters;

    /**
     * 创建默认结果转换器选择器。
     *
     * @param converters 可用结果转换器集合
     */
    public DefaultHandlerMethodResultConverterSelector(Collection<? extends HandlerMethodResultConverter<HC>> converters) {
        List<HandlerMethodResultConverter<HC>> sorted = new ArrayList<>(converters);
        sorted.sort(Comparator.comparingInt(HandlerMethodResultConverter<HC>::priority).reversed());
        this.converters = List.copyOf(sorted);
    }

    /**
     * 创建默认结果转换器选择器构建器。
     *
     * @param <HC> Handler 上下文类型
     * @return 构建器
     */
    public static <HC extends HandlerContext<?>> Builder<HC> builder() {
        return new Builder<>();
    }

    /**
     * 为指定 Handler 方法返回类型选择转换器。
     *
     * @param method Handler 方法
     * @return 支持该返回类型的转换器
     * @throws IllegalStateException 没有转换器支持该返回类型时抛出
     */
    @Override
    public HandlerMethodResultConverter<HC> select(Method method) {
        Class<?> resultType = method.getReturnType();
        for (HandlerMethodResultConverter<HC> converter : converters) {
            if (converter.supports(method, resultType)) {
                return converter;
            }
        }
        throw new IllegalStateException(
                "No HandlerMethodResultConverter supports return type '%s' of method %s"
                        .formatted(resultType.getName(), method.toGenericString())
        );
    }

    /**
     * {@link DefaultHandlerMethodResultConverterSelector} 构建器。
     *
     * @param <HC> Handler 上下文类型
     */
    public static class Builder<HC extends HandlerContext<?>> {
        private final List<HandlerMethodResultConverter<HC>> converters = new ArrayList<>();

        private Builder() {
        }

        /**
         * 追加一个结果转换器。
         *
         * @param converter 结果转换器
         * @return 当前构建器
         */
        public Builder<HC> converter(HandlerMethodResultConverter<HC> converter) {
            converters.add(converter);
            return this;
        }

        /**
         * 批量追加结果转换器。
         *
         * @param converters 结果转换器集合
         * @return 当前构建器
         */
        public Builder<HC> converters(Collection<? extends HandlerMethodResultConverter<HC>> converters) {
            this.converters.addAll(converters);
            return this;
        }

        /**
         * 构建默认结果转换器选择器。
         *
         * @return 默认结果转换器选择器
         */
        public DefaultHandlerMethodResultConverterSelector<HC> build() {
            return new DefaultHandlerMethodResultConverterSelector<>(converters);
        }
    }
}
