package com.nebilux.nexalithic.server;

import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.NexalithicEndpointBuilder;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.nebilux.nexalithic.core.infra.loadbalance.P2CBalancer;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.io.codec.assembler.BusinessPacketsAssembler;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.nebilux.nexalithic.core.messaging.task.NexalithicTask;
import com.nebilux.nexalithic.core.messaging.task.TaskHandle;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.SessionAttachment;
import com.nebilux.nexalithic.server.io.accept.AcceptorLoop;
import com.nebilux.nexalithic.server.io.accept.AdmissionStrategy;
import com.nebilux.nexalithic.server.io.handshake.HandshakeIngress;
import com.nebilux.nexalithic.server.io.handshake.HandshakeLoop;
import com.nebilux.nexalithic.server.io.session.ServiceUnit;
import com.nebilux.nexalithic.server.lifecycle.ServerLifecycleCoordinator;
import com.nebilux.nexalithic.server.messaging.ServerHandlerContext;
import com.nebilux.nexalithic.server.messaging.ServerHandlerCoordinator;
import com.nebilux.nexalithic.server.routing.NetworkRouter;
import com.nebilux.nexalithic.server.routing.NetworkRouterConfigurer;
import com.nebilux.nexalithic.server.security.ServerSecurityPolicy;
import com.nebilux.nexalithic.server.session.ServerSession;
import com.nebilux.nexalithic.server.session.SessionRegistry;
import com.nebilux.nexalithic.server.session.access.ChannelAccessCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Nexalithic 服务器
 *
 * @author Reonvia
 * @since 0.1.0
 */
@SuppressWarnings("UnusedReturnValue")
public class NexalithicServer extends NexalithicEndpoint {
    public static final Modules MODULES = new Modules();
    public static final class Modules extends NexalithicEndpoint.Modules {
        public final NexalithicModule<SessionRegistry> SessionRegistry = defineModule(SessionRegistry.class);
        public final NexalithicModule<NetworkRouter> NetworkRouter = defineModule(NetworkRouter.class);
        public final NexalithicModule<ChannelAccessCoordinator> ChannelAccessCoordinator = defineModule(ChannelAccessCoordinator.class);
        private Modules() {
            super(NexalithicServer.class);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NexalithicServer.class);
    private final AcceptorLoop acceptorLoop;
    private final SessionRegistry sessionRegistry;
    private final NetworkRouter networkRouter;

    private NexalithicServer(NexalithicBuilderContext context) {
        super(context, MODULES);
        this.acceptorLoop = context.getModule(ServerLifecycleCoordinator.MODULES.AcceptorLoop);
        this.sessionRegistry = context.getModule(MODULES.SessionRegistry);
        this.networkRouter = context.getModule(MODULES.NetworkRouter);
        System.gc();
    }

    public static Builder builder() {
        logger.info(Banner.render(NexalithicServer.class));
        return new Builder();
    }
    public static NexalithicServer unsafeCreate(NexalithicBuilderContext context) {
        return new NexalithicServer(context);
    }

    public int open(NexalithicChannel.Kind channelKind, InetSocketAddress localAddress, AdmissionStrategy.Builder<?> strategyBuilder) throws IOException {
        try {
            Objects.requireNonNull(channelKind, "channelKind");
            Objects.requireNonNull(localAddress, "localAddress");
            Objects.requireNonNull(strategyBuilder, "strategyBuilder");
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            int bindPort = serverSocketChannel.bind(localAddress).socket().getLocalPort();
            logger.info("Successfully bound server to [{}:{}] with ChannelKind [{}] and Strategy [{}]",
                    localAddress.getHostString(), bindPort, channelKind, strategyBuilder.getName());
            if (!acceptorLoop.register(serverSocketChannel, channelKind, strategyBuilder)) {
                throw new IOException("AcceptorLoop is not accepting listener registrations");
            }
            return bindPort;
        } catch (IOException exception) {
            logger.error("Failed to bind to local [{}]. ChannelKind [{}], Strategy [{}]",
                    localAddress, channelKind, strategyBuilder.getName(), exception);
            throw exception;
        }
    }
    public int open(NexalithicChannel.Kind kind, InetSocketAddress address) throws IOException {
        return open(kind, address, AdmissionStrategy.builder(AdmissionStrategy.Mode.DIRECT));
    }

    public boolean kick(String sessionName) {
        ServerSession session = sessionRegistry.removeSession(sessionName);
        if (session == null) {
            return false;
        }
        session.close();
        return true;
    }

    public TaskHandle submit(String sessionName, NexalithicTask.Builder taskBuilder) {
        ServerSession session = sessionRegistry.getSession(sessionName);
        if (session == null) {
            return null;
        }
        return session.getTaskCoordinator().submit(taskBuilder);
    }

    /**
     * 向指定的用户推送数据包
     *
     * @param sessionName 目标会话名
     * @param packet 业务数据包
     */
    public boolean push(String sessionName, BusinessPacket packet) {
        ServerSession session = sessionRegistry.getSession(sessionName);
        if (session == null) {
            return false;
        }
        return session.pushBusinessPacket(packet);
    }

    /**
     * 向所有已命名（已登录）的用户推送数据包
     *
     * @param packet 业务数据包
     */
    public void pushToAll(BusinessPacket packet) {
        packet.seal();
        sessionRegistry.forEachNamedSession(session -> session.pushBusinessPacket(packet.duplicate()));
    }

    /**
     * 遍历所有会话的业务附件
     * @param action 业务处理逻辑
     */
    public void forEachSession(Consumer<SessionAttachment> action) {
        sessionRegistry.forEachNamedSession(session -> action.accept(session.attachment()));
    }

    public Collection<String> getAllSessionsName() {
        return sessionRegistry.allSessionName();
    }

    /**
     * 返回全局网络路由器。
     *
     * <p>{@link #open} 成功把监听器注册到 AcceptorLoop 后会自动添加监听路由，关闭监听器时
     * 自动移除。调用者仍可在启动前或运行期间添加 CIDR/默认路由；显式规则优先于自动路由。</p>
     */
    public NetworkRouter getNetworkRouter() {
        return networkRouter;
    }

    public static class Builder extends NexalithicEndpointBuilder<Builder, ServerHandlerContext> {
        public Builder() {
            super(ServerHandlerContext.class);
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * 集中配置服务器的 CIDR 路由和默认路由。
         *
         * @param configurer 路由配置器
         * @return 当前 Builder
         */
        public Builder networkRouterConfigurer(NetworkRouterConfigurer configurer) {
            NetworkRouter router = context.getModule(MODULES.NetworkRouter, NetworkRouter::new);
            configurer.configure(router);
            return this;
        }

        public Builder securityPolicy(ServerSecurityPolicy securityPolicy) {
            context.setModule(NexalithicServer.MODULES.SecurityPolicy, securityPolicy);
            return this;
        }

        public NexalithicServer build() throws Throwable {
            return build(false);
        }
        public NexalithicServer build(boolean showOptions) throws Throwable {
            if (showOptions) {
                logger.trace("NexalithicServer-Options\n{}", OptionsDefinition.toString("com.nebilux.nexalithic", context));
            }

            controllerHandlerAssemblyBuilder.build().assembleInto(handlerRegistryBuilder);
            context.setModule(MODULES.EventBus, new NexalithicEventBus());
            context.setModule(MODULES.SessionRegistry, new SessionRegistry(context));
            context.getModule(MODULES.NetworkRouter, NetworkRouter::new);
            context.setModule(MODULES.ChannelAccessCoordinator, new ChannelAccessCoordinator(context));
            context.setModule(BusinessPacketsAssembler.MODULES.PayloadRegistry, payloadRegistryBuilder.build());
            context.setModule(HandlerCoordinator.MODULES.HandlerRegistry, handlerRegistryBuilder.build());
            ServerHandlerCoordinator handlerCoordinator = new ServerHandlerCoordinator(context);
            context.setModule(MODULES.HandlerCoordinator, handlerCoordinator);
            context.setModule(MODULES.TaskScheduler, new TaskScheduler(context));

            ServiceUnit[] serviceUnits = new ServiceUnit[context.getOption(ServerLifecycleCoordinator.OPTIONS.ServiceUnit_Count)];
            for (int i = 0; i < serviceUnits.length; i++) {
                serviceUnits[i] = new ServiceUnit(context);
            }
            context.setModule(ServerLifecycleCoordinator.MODULES.ServiceUnitLoadBalancer, new P2CBalancer<>(serviceUnits));

            HandshakeLoop[] handshakeLoops = new HandshakeLoop[context.getOption(ServerLifecycleCoordinator.OPTIONS.HandshakeLoop_Count)];
            for (int i = 0; i < handshakeLoops.length; i++) {
                handshakeLoops[i] = new HandshakeLoop(context);
            }

            LoadBalancer<Void, HandshakeLoop> handshakeLoopLoadBalancer = new P2CBalancer<>(handshakeLoops);
            context.setModule(AcceptorLoop.MODULES.HandshakeIngress, new HandshakeIngress() {
                private final LoadBalancer<Void, HandshakeLoop> loopLoadBalancer = handshakeLoopLoadBalancer;
                @Override
                public boolean submit(NexalithicChannel.Kind kind, SocketChannel channel) {
                    return loopLoadBalancer.select(null).submit(kind, channel);
                }
            });
            context.setModule(ServerLifecycleCoordinator.MODULES.HandshakeLoopLoadBalancer, handshakeLoopLoadBalancer);

            context.setModule(ServerLifecycleCoordinator.MODULES.AcceptorLoop, new AcceptorLoop(context));
            context.setModule(MODULES.LifecycleCoordinator, new ServerLifecycleCoordinator(context));
            return new NexalithicServer(context);
        }
    }
}
