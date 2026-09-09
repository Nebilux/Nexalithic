package com.thezeroer.nexalithic.server;

import com.thezeroer.nexalithic.core.NexalithicEndpoint;
import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.NexalithicEndpointBuilder;
import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.event.NexalithicEventBus;
import com.thezeroer.nexalithic.core.infra.loadbalance.P2CBalancer;
import com.thezeroer.nexalithic.core.io.codec.assembler.BusinessPacketsAssembler;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.thezeroer.nexalithic.core.messaging.task.NexalithicTask;
import com.thezeroer.nexalithic.core.messaging.task.TaskHandle;
import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.session.SessionAttachment;
import com.thezeroer.nexalithic.server.lifecycle.ServerLifecycleManager;
import com.thezeroer.nexalithic.server.lifecycle.accept.AcceptorLoop;
import com.thezeroer.nexalithic.server.lifecycle.accept.FiltrationStrategy;
import com.thezeroer.nexalithic.server.lifecycle.handshake.HandshakeLoop;
import com.thezeroer.nexalithic.server.lifecycle.service.ServiceUnit;
import com.thezeroer.nexalithic.server.lifecycle.service.session.ServerSession;
import com.thezeroer.nexalithic.server.manager.NetworkRouter;
import com.thezeroer.nexalithic.server.manager.SessionsManager;
import com.thezeroer.nexalithic.server.messaging.ServerHandlerContext;
import com.thezeroer.nexalithic.server.messaging.ServerHandlerCoordinator;
import com.thezeroer.nexalithic.server.security.ServerSecurityPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
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

    /**
     * 使用默认的旁路过滤策略绑定并监听指定地址。
     * <p>此方法等同于调用 {@link #open(AbstractPacket.PacketType, InetSocketAddress, FiltrationStrategy)}
     * 并传入 {@link FiltrationStrategy.Bypass}。适用于无需在接入层进行任何安全性或业务校验的场景。</p>
     *
     * @param packetType    绑定的协议包类型，决定了该端口接收数据后的解包逻辑。
     * @param local 监听的套接字地址（包含主机名和端口）。
     * @return 实际绑定的本地端口号。
     * @throws IOException 如果打开或绑定 ServerSocketChannel 失败。
     */
    public int open(AbstractPacket.PacketType packetType, InetSocketAddress local) throws IOException {
        return open(packetType, local, new FiltrationStrategy.Bypass());
    }
    /**
     * 绑定协议类型与监听地址，并配置特定的接入过滤策略。
     * <p><b>核心流程：</b>
     * <ol>
     * <li>同步打开并绑定 {@link ServerSocketChannel} 到指定地址。</li>
     * <li>获取实际分配的端口（尤其是当传入端口为 0 时，系统将自动分配空闲端口）。</li>
     * <li>将 Channel 及其策略封装并异步分发至 {@code AcceptorLoop}。</li>
     * </ol>
     * </p>
     *
     * <p><b>所有权转移：</b><br>
     * 方法成功返回后，{@code serverSocketChannel} 的生命周期管理权正式移交给内部的 {@code AcceptorLoop}。
     * 除非发生严重异常，否则外部调用者不应尝试关闭该 Channel。</p>
     *
     * @param packetType     协议包类型枚举。不能为空。
     * @param local  监听地址。如果端口号为 0，系统将选择一个临时端口。
     * @param strategy 自定义的过滤策略。不能为空，如需跳过过滤请显式传入 {@link FiltrationStrategy.Bypass}。
     * @return 实际监听的本地端口号。
     * @throws NullPointerException 如果 packetType 或 strategy 为 null。
     * @throws IOException         如果资源初始化失败或无法绑定到指定地址。
     */
    public int open(AbstractPacket.PacketType packetType, InetSocketAddress local, FiltrationStrategy strategy) throws IOException {
        try {
            if (packetType == null) {
                throw new IllegalStateException("packetType is null");
            }
            if (strategy == null) {
                throw new IllegalStateException("filtrationStrategy is null");
            }
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            int bindPort = serverSocketChannel.bind(local, 2048).socket().getLocalPort();
            logger.info("Successfully bound server to [{}:{}] with packetType [{}] and strategy [{}]",
                    local.getHostString(), bindPort, packetType, strategy.getName());
            lifecycleManager.getAcceptorLoop().dispatch(packetType, serverSocketChannel, strategy);
            return bindPort;
        } catch (IOException e) {
            logger.error("Failed to bind to local [{}]. packetType [{}], Strategy [{}]",
                    local, packetType, strategy.getName(), e);
            throw e;
        }
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
     * <li><b>前置性：</b> 开发者必须在调用 {@link #open(AbstractPacket.PacketType, InetSocketAddress, FiltrationStrategy)} 开启端口监听<b>之前</b>，
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

        public Builder addRoute(AbstractPacket.PacketType type, String cidr, int port) throws UnknownHostException {
            NetworkRouter router = context.getModule(MODULES.NetworkRouter, NetworkRouter::new);
            router.addRoute(type, cidr, port);
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
                logger.trace("NexalithicServer-Options\n{}", OptionsDefinition.toString("com.thezeroer.nexalithic", context));
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
                serviceUnits[i] = new ServiceUnit(context).addIdToLoopName(String.valueOf(i));
            }
            context.setModule(ServerLifecycleManager.MODULES.ServiceUnitLoadBalancer, new P2CBalancer<>(serviceUnits));

            HandshakeLoop[] handshakeLoops = new HandshakeLoop[context.getOption(ServerLifecycleManager.OPTIONS.HandshakeLoop_Count)];
            for (int i = 0; i < handshakeLoops.length; i++) {
                handshakeLoops[i] = (HandshakeLoop) new HandshakeLoop(context).addIdToName(String.valueOf(i));
            }
            context.setModule(ServerLifecycleManager.MODULES.HandshakeLoopLoadBalancer, new P2CBalancer<>(handshakeLoops));

            context.setModule(ServerLifecycleManager.MODULES.AcceptorLoop, (AcceptorLoop) new AcceptorLoop(context).addIdToName("0"));
            context.setModule(MODULES.LifecycleManager, new ServerLifecycleManager(context));
            return new NexalithicServer(context);
        }
    }
}
