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
import com.nebilux.nexalithic.server.lifecycle.ServerLifecycleManager;
import com.nebilux.nexalithic.server.manager.NetworkRouter;
import com.nebilux.nexalithic.server.manager.SessionsManager;
import com.nebilux.nexalithic.server.messaging.ServerHandlerContext;
import com.nebilux.nexalithic.server.messaging.ServerHandlerCoordinator;
import com.nebilux.nexalithic.server.security.ServerSecurityPolicy;
import com.nebilux.nexalithic.server.session.ServerSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Nexalithic 服务器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/02
 * @version 1.0.0
 */
@SuppressWarnings("UnusedReturnValue")
public class NexalithicServer extends NexalithicEndpoint<ServerLifecycleManager> {
    public static final Modules MODULES = new Modules();
    public static final class Modules extends NexalithicEndpoint.Modules {
        public final NexalithicModule<SessionsManager> SessionsManager = defineModule(SessionsManager.class);
        public final NexalithicModule<NetworkRouter> NetworkRouter = defineModule(NetworkRouter.class);
        private Modules() {
            super(NexalithicServer.class);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NexalithicServer.class);
    private final SessionsManager sessionsManager;
    private final NetworkRouter networkRouter;

    private NexalithicServer(NexalithicBuilderContext context) {
        super(context.getModule(MODULES.LifecycleManager), context.getModule(MODULES.EventBus));
        this.sessionsManager = context.getModule(MODULES.SessionsManager);
        this.networkRouter = context.getModule(MODULES.NetworkRouter);
        System.gc();
    }

    public static Builder builder() {
        logger.info(Banner.BANNER.formatted("Server"));
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
            logger.info("Successfully bound server to [{}:{}] with packetType [{}] and strategy [{}]",
                    localAddress.getHostString(), bindPort, channelKind, strategyBuilder.getName());
            lifecycleManager.getAcceptorLoop().register(serverSocketChannel, channelKind, strategyBuilder);
            return bindPort;
        } catch (IOException exception) {
            logger.error("Failed to bind to local [{}]. packetType [{}], Strategy [{}]",
                    localAddress, channelKind, strategyBuilder.getName(), exception);
            throw exception;
        }
    }
    public int open(NexalithicChannel.Kind kind, InetSocketAddress address) throws IOException {
        return open(kind, address, AdmissionStrategy.builder(AdmissionStrategy.Mode.DIRECT));
    }

    public boolean kick(String sessionName) {
        ServerSession session = sessionsManager.removeSession(sessionName);
        if (session == null) {
            return false;
        }
        session.close();
        return true;
    }

    public TaskHandle submit(String sessionName, NexalithicTask.Builder taskBuilder) {
        ServerSession session = sessionsManager.getSession(sessionName);
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
        ServerSession session = sessionsManager.getSession(sessionName);
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
        sessionsManager.forEachNamedSession(session -> session.pushBusinessPacket(packet.duplicate()));
    }

    /**
     * 遍历所有会话的业务附件
     * @param action 业务处理逻辑
     */
    public void forEachSession(Consumer<SessionAttachment> action) {
        sessionsManager.forEachNamedSession(session -> action.accept(session.attachment()));
    }

    public Collection<String> getAllSessionsName() {
        return sessionsManager.allSessionName();
    }

    /**
     * <p>获取当前服务器的路由管理器。</p>
     * <ul>
     * <li><b>前置性：</b> 开发者必须在调用 {@link #open(NexalithicChannel.Kind, InetSocketAddress, AdmissionStrategy.Builder)} 开启端口监听<b>之前</b>，
     * 通过此方法获取路由器并完成所有初始路由规则的添加（{@link NetworkRouter#addRoutes}）。</li>
     * <li><b>冷启动保护：</b> 若在 open 之后才添加路由，可能会导致服务器启动瞬间涌入的Channel
     * 因找不到匹配端口（Return -1）而触发静默丢弃或连接断开。</li>
     * <li><b>动态性：</b> 服务器运行期间仍支持动态增删路由，但基础骨干路由应在 open 前就位。</li>
     * </ul>
     *
     * @return 全局唯一的网络路由器实例 {@link NetworkRouter}
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

        public Builder addRoute(NexalithicChannel.Kind kind, String cidr, int port) throws UnknownHostException {
            NetworkRouter router = context.getModule(MODULES.NetworkRouter, NetworkRouter::new);
            router.addRoute(kind, cidr, port);
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
            context.setModule(MODULES.SessionsManager, new SessionsManager(context));
            context.setModule(BusinessPacketsAssembler.MODULES.PayloadRegistry, payloadRegistryBuilder.build());
            context.setModule(HandlerCoordinator.MODULES.HandlerRegistry, handlerRegistryBuilder.build());
            ServerHandlerCoordinator handlerCoordinator = new ServerHandlerCoordinator(context);
            context.setModule(MODULES.HandlerCoordinator, handlerCoordinator);
            context.setModule(MODULES.TaskScheduler, new TaskScheduler(context));

            ServiceUnit[] serviceUnits = new ServiceUnit[context.getOption(ServerLifecycleManager.OPTIONS.ServiceUnit_Count)];
            for (int i = 0; i < serviceUnits.length; i++) {
                serviceUnits[i] = new ServiceUnit(context);
            }
            context.setModule(ServerLifecycleManager.MODULES.ServiceUnitLoadBalancer, new P2CBalancer<>(serviceUnits));

            HandshakeLoop[] handshakeLoops = new HandshakeLoop[context.getOption(ServerLifecycleManager.OPTIONS.HandshakeLoop_Count)];
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
            context.setModule(ServerLifecycleManager.MODULES.HandshakeLoopLoadBalancer, handshakeLoopLoadBalancer);

            context.setModule(ServerLifecycleManager.MODULES.AcceptorLoop, new AcceptorLoop(context));
            context.setModule(MODULES.LifecycleManager, new ServerLifecycleManager(context));
            return new NexalithicServer(context);
        }
    }
}
