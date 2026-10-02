package com.nebilux.nexalithic.core;

import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.lifecycle.LifecycleManager;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.security.SecurityPolicy;

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
 * @param <LM> 具体终端使用的生命周期管理器类型
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class NexalithicEndpoint<LM extends LifecycleManager> {
    public static class Modules extends ModulesDefinition {
        public final NexalithicModule<LifecycleManager> LifecycleManager = defineModule(LifecycleManager.class);
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
        public static String render(Class<? extends NexalithicEndpoint<?>> endpointClass) {
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

        private static String resolveTitle(Class<? extends NexalithicEndpoint<?>> endpointClass, Package metadata) {
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
     * 终端生命周期管理器。
     *
     * <p>所有生命周期操作都会委托给该对象执行。具体终端可以在内部使用更精确的生命周期
     * 管理器类型，例如客户端的 {@code ClientLifecycleManager} 或服务端的
     * {@code ServerLifecycleManager}。</p>
     */
    protected final LM lifecycleManager;

    /**
     * 终端事件总线。
     *
     * <p>事件总线由 Builder 创建并注入，用于终端内部组件与用户侧监听逻辑之间传递事件。</p>
     */
    protected final NexalithicEventBus eventBus;

    /**
     * 创建终端基类。
     *
     * <p>该构造器由具体终端调用。传入的生命周期管理器和事件总线通常来自
     * {@link com.nebilux.nexalithic.core.builder.NexalithicBuilderContext} 中已经构建好的模块。</p>
     *
     * @param lifecycleManager 终端生命周期管理器
     * @param eventBus 终端事件总线
     */
    protected NexalithicEndpoint(LM lifecycleManager, NexalithicEventBus eventBus) {
        this.lifecycleManager = lifecycleManager;
        this.eventBus = eventBus;
    }

    /**
     * 启动终端。
     *
     * <p>该方法会委托给生命周期管理器执行实际启动流程。合法状态转换由
     * {@link LifecycleManager} 负责校验；如果当前状态不允许启动，将抛出
     * {@link IllegalStateException}。</p>
     */
    public final void start() {
        lifecycleManager.start();
    }

    /**
     * 停止终端。
     *
     * <p>该方法用于停止正在运行的终端，并释放运行期资源。合法状态转换由
     * {@link LifecycleManager} 负责校验。</p>
     */
    public final void stop() {
        lifecycleManager.stop();
    }

    /**
     * 关闭终端。
     *
     * <p>该方法用于触发终端的关闭流程。具体关闭语义由终端使用的
     * {@link LifecycleManager#onShutdown()} 实现决定。</p>
     */
    public final void shutdown() {
        lifecycleManager.shutdown();
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
     * <p>该状态直接来自生命周期管理器，可用于判断终端是否仍处于
     * {@link LifecycleManager.State#NEW}、{@link LifecycleManager.State#RUNNING}、
     * {@link LifecycleManager.State#TERMINATED} 或其他生命周期阶段。</p>
     *
     * @return 当前生命周期状态
     */
    public final LifecycleManager.State getLifecycleState() {
        return lifecycleManager.getState();
    }
}
