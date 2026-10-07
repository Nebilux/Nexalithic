package com.nebilux.nexalithic.core;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.lifecycle.LifecycleCoordinator;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.security.SecurityPolicy;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

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
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class NexalithicEndpoint {
    public static class Modules extends ModulesDefinition {
        public final NexalithicModule<LifecycleCoordinator> LifecycleCoordinator = defineModule(LifecycleCoordinator.class);
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
     * 终端生命周期协调器。
     *
     * <p>所有生命周期操作都会委托给该对象执行。协调器是终端基类的实现细节，
     * 具体终端需要访问的运行时组件应由其直接持有。</p>
     */
    private final LifecycleCoordinator lifecycleCoordinator;

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
     * 生命周期协调器和事件总线。</p>
     *
     * @param context 终端构建上下文
     * @param modules 具体终端的基础模块定义
     */
    protected NexalithicEndpoint(NexalithicBuilderContext context, Modules modules) {
        this.lifecycleCoordinator = context.getModule(modules.LifecycleCoordinator);
        this.eventBus = context.getModule(modules.EventBus);
    }

    /**
     * 启动终端。
     *
     * <p>该方法会阻塞当前线程，直到所有组件完成启动。需要非阻塞启动时，
     * 使用 {@link #startAsync()}。</p>
     */
    public final void start() {
        await(startAsync());
    }

    /**
     * 异步启动终端。
     *
     * <p>该方法只负责触发启动，不等待所有组件完成启动。合法状态转换由
     * {@link LifecycleCoordinator} 负责校验。</p>
     *
     * @return 终端启动完成阶段
     */
    public final CompletionStage<Void> startAsync() {
        return lifecycleCoordinator.start();
    }

    /**
     * 停止终端。
     *
     * <p>该方法会阻塞当前线程，直到终端完成立即终止并释放运行期资源。
     * 不得从终止过程所依赖的 Loop 线程中调用本方法；这种场景应使用
     * {@link #stopAsync()}，避免等待当前线程自身退出。</p>
     */
    public final void stop() {
        await(stopAsync());
    }

    /**
     * 异步停止终端。
     *
     * @return 终端终止完成阶段
     */
    public final CompletionStage<Void> stopAsync() {
        return lifecycleCoordinator.stop();
    }

    /**
     * 关闭终端。
     *
     * <p>该方法用于触发终端的优雅关闭流程；终端会先拒绝新工作，
     * 再等待已经接受的工作完成并释放资源。该方法会阻塞当前线程直到关闭完成；
     * 从终止过程所依赖的 Loop 线程中发起关闭时，应使用 {@link #shutdownAsync()}。</p>
     */
    public final void shutdown() {
        await(shutdownAsync());
    }

    /**
     * 异步优雅关闭终端。
     *
     * @return 终端优雅关闭完成阶段
     */
    public final CompletionStage<Void> shutdownAsync() {
        return lifecycleCoordinator.shutdown();
    }

    private static void await(CompletionStage<Void> completion) {
        try {
            completion.toCompletableFuture().join();
        } catch (CompletionException exception) {
            Throwable failure = exception.getCause() == null ? exception : exception.getCause();
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException("Lifecycle operation failed", failure);
        }
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

    /**
     * 返回当前生命周期状态。
     *
     * <p>该状态直接来自生命周期协调器，可用于判断终端是否仍处于
     * {@link LifecycleCoordinator.State#NEW}、{@link LifecycleCoordinator.State#RUNNING}、
     * {@link LifecycleCoordinator.State#TERMINATED} 或其他生命周期阶段。</p>
     *
     * @return 当前生命周期状态
     */
    public final LifecycleCoordinator.State getLifecycleState() {
        return lifecycleCoordinator.getState();
    }
}
