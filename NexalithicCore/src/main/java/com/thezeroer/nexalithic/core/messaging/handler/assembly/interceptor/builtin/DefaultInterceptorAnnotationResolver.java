package com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.builtin;

import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorAnnotationResolver;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorConfigurationBinding;
import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

import java.lang.annotation.Annotation;
import java.util.*;

/**
 * 默认拦截器注解解析器。
 *
 * <p>该实现维护一组 {@link InterceptorAnnotationParser}，并在构造时按照
 * {@link InterceptorAnnotationParser#priority()} 从高到低排序。解析时会选择第一个
 * 同时支持目标注解类型和目标拦截器类型的解析器。</p>
 *
 * <p>解析结果会被包装为 {@link InterceptorConfigurationBinding}，
 * 供 {@link com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorFactory}
 * 创建实际拦截器实例。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/29
 */
public class DefaultInterceptorAnnotationResolver implements InterceptorAnnotationResolver {
    private final List<InterceptorAnnotationParser> parsers;

    /**
     * 创建默认解析器。
     *
     * @param parsers 可用的注解解析器列表
     */
    public DefaultInterceptorAnnotationResolver(List<InterceptorAnnotationParser> parsers) {
        List<InterceptorAnnotationParser> sorted = new ArrayList<>(parsers);
        sorted.sort(Comparator.comparingInt(InterceptorAnnotationParser::priority).reversed());
        this.parsers = List.copyOf(sorted);
    }

    /**
     * 创建默认解析器构建器。
     *
     * @return 构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 使用已注册解析器解析一条拦截器注解绑定。
     *
     * @param annotation 业务注解实例
     * @param interceptorType 该业务注解绑定的拦截器类型
     * @return 解析后的拦截器配置绑定
     * @throws IllegalStateException 没有任何解析器支持该绑定时抛出
     */
    @Override
    public InterceptorConfigurationBinding resolve(Annotation annotation, Class<? extends HandlerInterceptor<?>> interceptorType) {
        for (InterceptorAnnotationParser parser : parsers) {
            if (parser.supports(annotation.annotationType(), interceptorType)) {
                return new InterceptorConfigurationBinding(parser.parse(annotation, interceptorType), interceptorType);
            }
        }
        throw new IllegalStateException("No InterceptorAnnotationParser supports binding '%s -> %s'"
                .formatted(annotation.annotationType().getName(), interceptorType.getName())
        );
    }

    /**
     * {@link DefaultInterceptorAnnotationResolver} 构建器。
     */
    public static class Builder {
        private final List<InterceptorAnnotationParser> parsers = new ArrayList<>();

        /**
         * 追加一个注解解析器。
         *
         * @param parser 注解解析器
         * @return 当前构建器
         */
        public Builder parser(InterceptorAnnotationParser parser) {
            parsers.add(parser);
            return this;
        }

        /**
         * 批量追加注解解析器。
         *
         * @param parsers 注解解析器列表
         * @return 当前构建器
         */
        public Builder parsers(List<InterceptorAnnotationParser> parsers) {
            this.parsers.addAll(parsers);
            return this;
        }

        /**
         * 构建默认注解解析器。
         *
         * @return 默认注解解析器
         */
        public DefaultInterceptorAnnotationResolver build() {
            return new DefaultInterceptorAnnotationResolver(parsers);
        }
    }
}
