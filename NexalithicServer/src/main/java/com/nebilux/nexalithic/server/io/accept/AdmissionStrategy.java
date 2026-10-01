package com.nebilux.nexalithic.server.io.accept;

import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.server.io.handshake.HandshakeIngress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 连接准入策略。
 *
 * <p>负责协调准入任务调度、上下文生命周期、过滤器顺序执行、异常传播以及通过过滤后的
 * 连接移交。策略本身不执行握手；连接通过所有过滤器后，经由框架注入的
 * {@link HandshakeIngress} 提交给握手阶段。</p>
 *
 * <h2>连接所有权</h2>
 * <ul>
 *     <li>
 *         调用 {@link #submit(NexalithicChannel.Kind, SocketChannel)} 前，
 *         SocketChannel 的所有权属于调用方。
 *     </li>
 *     <li>
 *         Dispatcher 成功接收任务后，连接由 AdmissionStrategy 管理。
 *     </li>
 *     <li>
 *         过滤失败、调度被拒绝或握手入口拒绝时，策略负责关闭连接。
 *     </li>
 *     <li>
 *         HandshakeIngress 成功接收连接后，连接所有权转移到握手阶段。
 *     </li>
 * </ul>
 *
 * <h2>上下文生命周期</h2>
 * <p>策略通过 {@link AdmissionContextAllocator} 为一次准入过程获取上下文。
 * 同一个上下文会传递给本次准入过程中的所有过滤器，并在流程结束后释放。</p>
 *
 * <h2>并发约束</h2>
 * <p>当 {@link AdmissionDispatcher} 采用异步并发执行时，Filter、Allocator
 * 以及用户提供的上下文都必须满足对应的线程安全要求。</p>
 *
 * @param <C> 准入上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class AdmissionStrategy<C> {
    /**
     * 连接准入模式。
     *
     * <p>该模式仅用于在构建阶段选择具体的准入策略实现，策略创建后不可更改。
     * 它描述连接移交给握手阶段前是否执行准入过滤，不表示握手本身采用同步或异步方式。</p>
     */
    public enum Mode {
        /**
         * 直接准入模式。
         *
         * <p>不经过 {@link AdmissionDispatcher}，不获取准入上下文，
         * 也不执行任何 {@link AdmissionFilter}，而是直接尝试将连接提交给
         * {@link HandshakeIngress}。</p>
         */
        DIRECT,
        /**
         * 过滤准入模式。
         *
         * <p>先通过 {@link AdmissionDispatcher} 调度准入任务，然后获取本次准入上下文，
         * 按注册顺序执行过滤器。所有过滤器通过后，连接才会提交给
         * {@link HandshakeIngress}；流程结束后释放准入上下文。</p>
         *
         * <p>该模式可以同步执行，也可以异步执行，具体取决于配置的
         * {@link AdmissionDispatcher}。</p>
         */
        FILTER
    }
    private static final Logger logger = LoggerFactory.getLogger(AdmissionStrategy.class);
    private final HandshakeIngress ingress;
    protected final String name;

    private AdmissionStrategy(HandshakeIngress ingress, String name) {
        this.ingress = ingress;
        this.name = name == null ? toString() : name;
    }

    /**
     * 创建准入策略构建器。
     *
     * @return 新的构建器
     */
    public static <C> Builder<C> builder(Mode mode) {
        return new Builder<>(mode);
    }

    /**
     * 提交一个已经由监听器接受的连接。
     *
     * <p>返回 {@code true} 仅表示 Dispatcher 接收了准入任务，
     * 不表示连接已经通过过滤，也不表示握手已经完成。</p>
     *
     * @param kind    待准入连接的通道种类
     * @param channel 待准入的通道
     */
    public abstract void submit(NexalithicChannel.Kind kind, SocketChannel channel);

    /**
     * 返回策略名称。
     *
     * @return 策略名称
     */
    public String getName() {
        return name;
    }

    protected void approve(NexalithicChannel.Kind kind, SocketChannel channel) {
        try {
            if (ingress.submit(kind, channel)) {
                return;
            }
        } catch (Exception exception) {
            logger.error("[{}] failed to submit handshake", name, exception);
        }
        reject(channel);
    }
    protected void reject(SocketChannel channel) {
        try {
            channel.close();
        } catch (Exception exception) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] failed to close rejected channel", name, exception);
            }
        }
    }

    private static class Direct<C> extends AdmissionStrategy<C> {
        private Direct(HandshakeIngress ingress, String name) {
            super(ingress, name == null ? (AdmissionStrategy.class.getSimpleName() + "#" + Direct.class.getSimpleName()) : name);
        }

        @Override
        public void submit(NexalithicChannel.Kind kind, SocketChannel channel) {
            approve(kind, channel);
        }
    }

    private static class Filter<C> extends AdmissionStrategy<C> {
        private final List<AdmissionFilter<C>> filters;
        private final AdmissionContextAllocator<C> allocator;
        private final AdmissionDispatcher dispatcher;
        private final AdmissionExecutor executor;

        private Filter(HandshakeIngress ingress, String name, Collection<AdmissionFilter<C>> filters, AdmissionContextAllocator<C> allocator, AdmissionDispatcher dispatcher) {
            super(ingress, name);
            this.filters = List.copyOf(filters);
            this.allocator = allocator;
            this.dispatcher = dispatcher;
            this.executor = this::evaluate;
        }

        @Override
        public void submit(NexalithicChannel.Kind kind, SocketChannel channel) {
            try {
                if (dispatcher.dispatch(executor, kind, channel)) {
                    return;
                }
            } catch (Exception exception) {
                logger.error("[{}] failed to dispatch admission", name, exception);
            }
            reject(channel);
        }

        private void evaluate(NexalithicChannel.Kind kind, SocketChannel channel) {
            C context;
            try {
                context = allocator.acquire(kind, channel);
            } catch (Exception exception) {
                logger.error("Failed to acquire admission context", exception);
                reject(channel);
                return;
            }
            try {
                Exception failure = null;
                for (AdmissionFilter<C> filter : filters) {
                    if (failure == null) {
                        try {
                            if (!filter.allow(kind, channel, context)) {
                                reject(channel);
                                return;
                            }
                        } catch (Exception exception) {
                            failure = exception;
                        }
                        continue;
                    }
                    try {
                        switch (filter.onFailure(kind, channel, context, failure)) {
                            case RECOVER -> failure = null;
                            case REJECT -> {
                                reject(channel);
                                return;
                            }
                            case PROPAGATE -> {}
                        }
                    } catch (Exception exception) {
                        if (exception != failure) {
                            exception.addSuppressed(failure);
                        }
                        failure = exception;
                    }
                }
                if (failure != null) {
                    reject(channel);
                    return;
                }
                approve(kind, channel);
            } finally {
                try {
                    allocator.release(context);
                } catch (Exception exception) {
                    logger.error("Failed to release admission context", exception);
                }
            }
        }
    }

    /**
     * 准入策略构建器。
     *
     * <p>用户通过 Builder 配置过滤器、上下文分配器、调度器及策略名称。
     * {@link HandshakeIngress} 由框架在创建运行时策略时提供。</p>
     *
     * @param <C> 准入上下文类型
     */
    public static class Builder<C> {
        private final Mode mode;
        /** 可变过滤器配置集合。 */
        private Collection<AdmissionFilter<C>> filters;
        /** 上下文分配器。 */
        private AdmissionContextAllocator<C> allocator;
        /** 准入任务调度器。 */
        private AdmissionDispatcher dispatcher;
        /** 策略名称。 */
        private String name;

        /**
         * 创建使用默认配置的 Builder。
         *
         * <p>默认不创建上下文，并在提交线程中同步执行准入流程。</p>
         */
        private Builder(Mode mode) {
            this.mode = mode;
            filters = new ArrayList<>();
            allocator = (kind, channel) -> null;
            dispatcher = (executor, kind, channel) -> {
                executor.execute(kind, channel);
                return true;
            };
        }

        /**
         * 替换过滤器集合。
         *
         * @param filters 新的过滤器集合
         * @return 当前 Builder
         */
        public Builder<C> filters(Collection<AdmissionFilter<C>> filters) {
            this.filters = filters;
            return this;
        }

        /**
         * 设置上下文分配器。
         *
         * @param allocator 上下文分配器
         * @return 当前 Builder
         */
        public Builder<C> allocator(AdmissionContextAllocator<C> allocator) {
            this.allocator = allocator;
            return this;
        }

        /**
         * 设置准入任务调度器。
         *
         * @param dispatcher 调度器
         * @return 当前 Builder
         */
        public Builder<C> dispatcher(AdmissionDispatcher dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        /**
         * 批量追加过滤器。
         *
         * @param filters 需要追加的过滤器
         * @return 当前 Builder
         */
        public Builder<C> addFilters(Collection<AdmissionFilter<C>> filters) {
            this.filters.addAll(filters);
            return this;
        }

        /**
         * 追加一个过滤器。
         *
         * @param filter 需要追加的过滤器
         * @return 当前 Builder
         */
        public Builder<C> addFilter(AdmissionFilter<C> filter) {
            this.filters.add(filter);
            return this;
        }

        /**
         * 设置策略名称。
         *
         * @param name 策略名称
         * @return 当前 Builder
         */
        public Builder<C> name(String name) {
            this.name = name;
            return this;
        }

        public String getName() {
            return name;
        }

        /**
         * 使用框架提供的握手入口创建运行时准入策略。
         *
         * @param ingress 握手阶段入口
         * @return 完整初始化的准入策略
         */
        public AdmissionStrategy<C> build(HandshakeIngress ingress) {
            return switch (mode) {
                case DIRECT -> new Direct<>(ingress, name);
                case FILTER -> new Filter<>(ingress, name, filters, allocator, dispatcher);
            };
        }
    }
}