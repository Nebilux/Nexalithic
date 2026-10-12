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

package com.nebilux.nexalithic.core;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.lifecycle.Lifecycle;
import com.nebilux.nexalithic.core.lifecycle.NexalithicLifecycleException;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.security.SecurityPolicy;

import java.util.concurrent.CompletionStage;

import static com.nebilux.nexalithic.core.exception.FailureSupport.unwrapCompletionException;

/**
 * Nexalithic 终端抽象基类。
 *
 * <p>终端表示一个已经构建完成、可以启动和关闭的 Nexalithic 运行实例。
 * {@code NexalithicServer} 与 {@code NexalithicClient} 都共享同一组基础能力：
 * 生命周期控制、生命周期状态查询以及事件总线访问。</p>
 *
 * <p>该类只承载 Server 与 Client 真正共同的运行时行为，不负责暴露网络监听、连接建立、
 * 会话管理、任务提交等终端特有能力。终端特有 API 应继续定义在具体子类中，避免基类被
 * Server/Client 的差异污染。</p>
 *
 * <p>普通用户通常不会直接继承或实例化该类，而是通过 {@code NexalithicServer.builder()}
 * 或 {@code NexalithicClient.builder()} 创建具体终端。</p>
 *
 * <p>生命周期操作统一使用 {@link Lifecycle} 的非阻塞 API。调用方若需等待，
 * 应等待 {@link #start()} 或 {@link #shutdown(ShutdownMode)} 返回的完成阶段。
 * 终端入口的异步失败以 {@link com.nebilux.nexalithic.core.lifecycle.NexalithicLifecycleException}
 * 呈现，底层原因保存在 cause 中。</p>
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class NexalithicEndpoint implements Lifecycle {
    public static class Modules extends ModulesDefinition {
        public final NexalithicModule<Lifecycle> Lifecycle = defineModule(Lifecycle.class);
        public final NexalithicModule<HandlerCoordinator<?, ?, ?>> HandlerCoordinator = defineModule(HandlerCoordinator.class);
        public final NexalithicModule<TaskScheduler> TaskScheduler = defineModule(TaskScheduler.class);
        public final NexalithicModule<SecurityPolicy> SecurityPolicy = defineModule(SecurityPolicy.class);
        public final NexalithicModule<NexalithicEventBus> EventBus = defineModule(NexalithicEventBus.class);
        protected Modules(Class<?> holder) {
            super(holder);
        }
    }

    public enum Type {
        CLIENT,
        SERVER
    }

    /**
     * Nexalithic 启动横幅。
     */
    public static final class Banner {
        private static final String LOGO =
                """
                 _   _                _ _ _   _     _
                | \\ | | _____  ____ _| (_) |_| |__ (_) ___
                |  \\| |/ _ \\ \\/ / _` | | | __| '_ \\| |/ __|
                | |\\  |  __/>  < (_| | | | |_| | | | | (__
                |_| \\_|\\___/_/\\_\\__,_|_|_|\\__|_| |_|_|\\___|
                """.stripTrailing();

        private static final int WIDTH = LOGO.lines().mapToInt(String::length).max().orElse(0);

        private Banner() {}

        /**
         * 根据具体终端类型生成启动横幅。
         *
         * @param endpointClass 具体终端类
         * @return 完整启动横幅
         */
        public static String render(Class<? extends NexalithicEndpoint> endpointClass) {
            Package metadata = endpointClass.getPackage();

            String title = resolveTitle(endpointClass, metadata);
            String version = resolveVersion(metadata);

            return System.lineSeparator()
                    + LOGO
                    + System.lineSeparator()
                    + System.lineSeparator()
                    + createMetadataLine(title, version)
                    + System.lineSeparator();
        }

        private static String resolveTitle(Class<? extends NexalithicEndpoint> endpointClass, Package metadata) {
            String title = metadata.getImplementationTitle();
            if (title != null && !title.isBlank()) {
                return title;
            }
            String className = endpointClass.getSimpleName();
            if (className.startsWith("Nexalithic")) {
                return "Nexalithic " + className.substring("Nexalithic".length());
            }
            return className;
        }
        private static String resolveVersion(Package metadata) {
            String version = metadata.getImplementationVersion();
            if (version == null || version.isBlank()) {
                return "DEV";
            }
            return version;
        }
        private static String createMetadataLine(String title, String version) {
            String left = ":: " + title + " ::";
            String right = "(v" + version + ")";
            int spacing = Math.max(1, WIDTH - left.length() - right.length());
            return left + " ".repeat(spacing) + right;
        }
    }

    /**
     * 终端生命周期实现。
     *
     * <p>所有生命周期操作都会委托给该对象执行。它是终端基类的实现细节，
     * 具体终端需要访问的运行时组件应由其直接持有。</p>
     */
    private final Lifecycle lifecycle;

    /**
     * 终端事件总线。
     *
     * <p>事件总线由 Builder 创建并注入，用于终端内部组件与用户侧监听逻辑之间传递事件。</p>
     */
    protected final NexalithicEventBus eventBus;

    /**
     * 创建终端基类。
     *
     * <p>该构造器由具体终端调用，并根据终端的模块定义从构建上下文中取得
     * 生命周期实现和事件总线。</p>
     *
     * @param context 终端构建上下文
     * @param modules 具体终端的基础模块定义
     */
    protected NexalithicEndpoint(NexalithicBuilderContext context, Modules modules) {
        this.lifecycle = context.getModule(modules.Lifecycle);
        this.eventBus = context.getModule(modules.EventBus);
    }

    /** {@inheritDoc} */
    @Override
    public final CompletionStage<Void> start() {
        return exposeLifecycleResult(lifecycle.start(), "start");
    }

    /** {@inheritDoc} */
    @Override
    public final CompletionStage<Void> shutdown(ShutdownMode mode) {
        return exposeLifecycleResult(lifecycle.shutdown(mode), "shut down");
    }

    /** {@inheritDoc} */
    @Override
    public final CompletionStage<Void> whenStarted() {
        return exposeLifecycleResult(lifecycle.whenStarted(), "start");
    }

    /** {@inheritDoc} */
    @Override
    public final CompletionStage<Void> whenTerminated() {
        return exposeLifecycleResult(lifecycle.whenTerminated(), "shut down");
    }

    /** {@inheritDoc} */
    @Override
    public final State lifecycleState() {
        return lifecycle.lifecycleState();
    }

    /** 仅在面向终端调用者的完成阶段上转换失败，保留内部生命周期的原始结果。 */
    private CompletionStage<Void> exposeLifecycleResult(CompletionStage<Void> result, String operation) {
        return result.handle((ignored, failure) -> {
            if (failure == null) {
                return null;
            }
            Throwable cause = unwrapCompletionException(failure);
            if (cause instanceof Error error) {
                throw error;
            }
            if (cause instanceof NexalithicLifecycleException lifecycleFailure) {
                throw lifecycleFailure;
            }
            throw NexalithicLifecycleException.forEndpointOperation(getClass(), operation, cause);
        });
    }

    /**
     * 返回终端事件总线。
     *
     * <p>用户可以通过事件总线订阅终端运行过程中发布的事件。事件类型由具体模块定义。</p>
     *
     * @return 当前终端使用的事件总线
     */
    public final NexalithicEventBus getEventBus() {
        return eventBus;
    }
}
