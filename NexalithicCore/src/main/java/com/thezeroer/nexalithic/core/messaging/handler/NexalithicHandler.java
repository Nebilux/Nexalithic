package com.thezeroer.nexalithic.core.messaging.handler;

import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;
import com.thezeroer.nexalithic.core.messaging.handler.interceptor.InterceptorPipeline;
import com.thezeroer.nexalithic.core.messaging.handler.interceptor.InterceptorPipelineFactory;
import com.thezeroer.nexalithic.core.messaging.handler.mapping.HandlerPathMatcher;
import com.thezeroer.nexalithic.core.messaging.task.NexalithicTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Nexalithic 业务消息处理器。
 *
 * <p>该类封装一个可注册、可匹配、可执行的业务处理单元。它由
 * {@link HandlerPathMatcher}、实际业务调用函数 {@link HandlerInvoker}、
 * 拦截器管线{@link InterceptorPipeline}以及 {@link HandlerMetadata} 组成。</p>
 *
 * <p>一次调用的执行顺序为：</p>
 * <ol>
 *     <li>正序执行所有 {@link HandlerInterceptor#onBefore(HandlerContext, HandlerMetadata)}；</li>
 *     <li>所有前置拦截器通过后执行 {@link HandlerInvoker}；</li>
 *     <li>业务正常完成后逆序执行 {@code onAfter}；</li>
 *     <li>最后逆序执行 {@code onCompletion}。</li>
 * </ol>
 *
 * <p>如果前置拦截器返回 {@code false}，业务函数不会被调用；
 * 如果业务函数抛出异常，异常会继续向调用方传播。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/15
 * @see NexalithicTask
 */
public class NexalithicHandler<HC extends HandlerContext<?>> {
    private static final Logger logger = LoggerFactory.getLogger(NexalithicHandler.class);
    private final HandlerInvoker<HC> invoker;
    private final InterceptorPipeline<HC> pipeline;
    private final HandlerMetadata metadata;

    private NexalithicHandler(HandlerInvoker<HC> invoker, InterceptorPipeline<HC> pipeline, HandlerMetadata metadata) {
        this.invoker = invoker;
        this.pipeline = pipeline;
        this.metadata = metadata;
    }

    /**
     * 创建空的 Handler 构建器。
     *
     * @param <HC> Handler 上下文类型
     * @return Handler 构建器
     */
    public static <HC extends HandlerContext<?>> Builder<HC> builder() {
        return new Builder<>();
    }

    /**
     * 使用路径匹配器和业务调用函数创建 Handler 构建器。
     *
     * @param pathMatcher Handler 注册路径
     * @param invoker 业务调用函数
     * @param <HC> Handler 上下文类型
     * @return Handler 构建器
     */
    public static <HC extends HandlerContext<?>> Builder<HC> builder(HandlerPathMatcher pathMatcher, HandlerInvoker<HC> invoker) {
        return new Builder<>(pathMatcher, invoker);
    }

    public void handle(HC context) throws Exception {
        try {
            if (!pipeline.applyBefore(context)) {
                return;
            }
        } catch (Exception e) {
            logger.warn("HandlerInterceptor.onBefore failed, {}", metadata, e);
            throw e;
        }
        try {
            invoker.invoke(context);
        } catch (Exception e) {
            pipeline.complete(context, e);
            logger.warn("HandlerInvoker.handle failed, {}", metadata, e);
            throw e;
        }
        try {
            pipeline.applyAfter(context);
            pipeline.complete(context, null);
        } catch (Exception e) {
            logger.warn("HandlerInterceptor.onAfter failed, {}", metadata, e);
            pipeline.complete(context, e);
        }
    }

    /**
     * 返回 Handler 元数据。
     *
     * @return Handler 元数据
     */
    public HandlerMetadata getMetadata() {
        return metadata;
    }

    /**
     * {@link NexalithicHandler} 构建器。
     *
     * <p>{@link #pathMatcher(HandlerPathMatcher)} 和 {@link #invoker(HandlerInvoker)}
     * 是必填项。名称和描述未指定时会在构建阶段填充默认值。</p>
     *
     * @param <HC> Handler 上下文类型
     */
    public static class Builder<HC extends HandlerContext<?>> {
        private HandlerPathMatcher pathMatcher;
        private HandlerInvoker<HC> invoker;
        private List<HandlerInterceptor<HC>> interceptors = new ArrayList<>();
        private String name, description;

        public Builder() {}

        /**
         * 使用必填项创建构建器。
         *
         * @param pathMatcher Handler 注册路径
         * @param invoker 业务调用函数
         */
        public Builder(HandlerPathMatcher pathMatcher, HandlerInvoker<HC> invoker) {
            this.pathMatcher = pathMatcher;
            this.invoker = invoker;
        }

        /**
         * 设置 Handler 注册路径匹配器。
         *
         * @param pathMatcher 注册到 {@link com.thezeroer.nexalithic.core.messaging.handler.mapping.HandlerRegistry} 的路径匹配器
         * @return 当前构建器
         */
        public Builder<HC> pathMatcher(HandlerPathMatcher pathMatcher) {
            this.pathMatcher = pathMatcher;
            return this;
        }

        /**
         * 设置实际业务调用函数。
         *
         * @param invoker 业务调用函数
         * @return 当前构建器
         */
        public Builder<HC> invoker(HandlerInvoker<HC> invoker) {
            this.invoker = invoker;
            return this;
        }

        /**
         * 追加一个拦截器。
         *
         * @param interceptor 拦截器实例
         * @return 当前构建器
         */
        public Builder<HC> interceptor(HandlerInterceptor<HC> interceptor) {
            this.interceptors.add(interceptor);
            return this;
        }

        /**
         * 追加多个拦截器。
         *
         * @param interceptors 拦截器数组
         * @return 当前构建器
         */
        @SafeVarargs
        public final Builder<HC> interceptors(HandlerInterceptor<HC>... interceptors) {
            Collections.addAll(this.interceptors, interceptors);
            return this;
        }

        /**
         * 追加一个拦截器列表。
         *
         * @param interceptors 拦截器列表
         * @return 当前构建器
         */
        public Builder<HC> interceptors(List<? extends HandlerInterceptor<HC>> interceptors) {
            this.interceptors.addAll(interceptors);
            return this;
        }

        /**
         * 设置 Handler 名称。
         *
         * @param name Handler 名称；为空时构建阶段会生成默认名称
         * @return 当前构建器
         */
        public Builder<HC> name(String name) {
            this.name = name;
            return this;
        }

        /**
         * 设置 Handler 描述。
         *
         * @param description Handler 描述；为 {@code null} 时构建阶段使用空字符串
         * @return 当前构建器
         */
        public Builder<HC> description(String description) {
            this.description = description;
            return this;
        }

        /**
         * 构建不可变 Handler 实例。
         *
         * @return 构建完成的 Handler
         * @throws IllegalArgumentException 缺少路径匹配器或业务调用函数时抛出
         */
        public NexalithicHandler<HC> build() {
            if (pathMatcher == null) {
                throw new IllegalArgumentException("pathMatcher is required");
            }
            if (invoker == null) {
                throw new IllegalArgumentException("invoker is required");
            }
            if (name == null || name.isEmpty()) {
                name = "Unnamed" + pathMatcher.formatPath();
            }
            if (description == null) {
                description = "";
            }
            HandlerMetadata metadata = new HandlerMetadata(name, description, pathMatcher);
            return new NexalithicHandler<>(invoker, InterceptorPipelineFactory.create(metadata, interceptors), metadata);
        }
    }
}
