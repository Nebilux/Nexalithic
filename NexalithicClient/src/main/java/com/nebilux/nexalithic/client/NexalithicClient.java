package com.nebilux.nexalithic.client;

import com.nebilux.nexalithic.client.lifecycle.ClientLifecycleManager;
import com.nebilux.nexalithic.client.io.session.ClientSessionLoop;
import com.nebilux.nexalithic.client.session.ClientSession;
import com.nebilux.nexalithic.client.manager.LinkStatusManager;
import com.nebilux.nexalithic.client.messaging.ClientHandlerContext;
import com.nebilux.nexalithic.client.messaging.ClientHandlerCoordinator;
import com.nebilux.nexalithic.client.security.ClientSecurityPolicy;
import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.NexalithicEndpointBuilder;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.io.codec.assembler.BusinessPacketsAssembler;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.nebilux.nexalithic.core.messaging.task.NexalithicTask;
import com.nebilux.nexalithic.core.messaging.task.TaskHandle;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.concurrent.locks.LockSupport;

/**
 * Nexalithic客户端
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/02
 * @version 1.0.0
 */
@SuppressWarnings("UnusedReturnValue")
public class NexalithicClient extends NexalithicEndpoint<ClientLifecycleManager> {
    public static final Modules MODULES = new Modules();
    public static final class Modules extends NexalithicEndpoint.Modules {
        public final NexalithicModule<LinkStatusManager> LinkStatusManager = defineModule(LinkStatusManager.class);
        private Modules() {
            super(NexalithicClient.class);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NexalithicClient.class);
    private final LinkStatusManager linkStatusManager;
    private final ClientSessionLoop clientSessionLoop;

    private NexalithicClient(NexalithicBuilderContext context) {
        super(context.getModule(MODULES.LifecycleManager), context.getModule(MODULES.EventBus));
        this.linkStatusManager = context.getModule(MODULES.LinkStatusManager);
        this.clientSessionLoop = context.getModule(ClientLifecycleManager.MODULES.SessionLoop);
        System.gc();
    }
    public static NexalithicClient unsafeCreate(NexalithicBuilderContext context) {
        return new NexalithicClient(context);
    }

    public static Builder builder() {
        logger.info(Banner.BANNER.formatted("Client"));
        return new Builder();
    }

    public boolean link(InetSocketAddress remote) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, ShortBufferException {
        if (linkStatusManager.getStatus() != LinkStatusManager.Status.UNLINKED) {
            throw new IllegalStateException("Cannot link while in State " + linkStatusManager.getStatus() + ", must be " + LinkStatusManager.Status.UNLINKED);
        }
        SocketChannel socketChannel = SocketChannel.open(remote);
        logger.info("Linking to [{}]", remote);
        linkStatusManager.trigger(LinkStatusManager.Status.LINKING, remote);
        clientSessionLoop.getNetworkRouter().setServerAddress(remote);
        try {
            if (clientSessionLoop.link(AbstractPacket.PacketType.Signaling, socketChannel, null)) {
                return true;
            } else {
                linkStatusManager.trigger(LinkStatusManager.Status.UNLINKED, LinkStatusManager.Reason.REMOTE_ACTIVE);
            }
        } catch (Exception e) {
            linkStatusManager.trigger(LinkStatusManager.Status.UNLINKED, e instanceof IOException ? LinkStatusManager.Reason.NETWORK_ERROR : LinkStatusManager.Reason.PROTOCOL_ERROR, e);
            throw e;
        }
        return false;
    }
    public void unlink() {
        clientSessionLoop.unlink();
    }

    public TaskHandle submit(NexalithicTask.Builder taskBuilder) {
        return getSession().getTaskCoordinator().submit(taskBuilder);
    }
    public boolean push(BusinessPacket packet) {
        return getSession().pushBusinessPacket(packet);
    }

    public LinkStatusManager.Status getLinkStatus() {
        return linkStatusManager.getStatus();
    }

    private ClientSession getSession() {
        ClientSession session = clientSessionLoop.getSession();
        if (session == null) {
            for (int i = 0; i < 100; i++) {
                if (session != null) {
                    return session;
                } else {
                    if (i < 50) {
                        Thread.onSpinWait();
                    } else {
                        LockSupport.parkNanos(i * 1_000_000L);
                    }
                }
                session = clientSessionLoop.getSession();
            }
        }
        return session;
    }

    public static class Builder extends NexalithicEndpointBuilder<Builder, ClientHandlerContext> {
        public Builder() {
            super(ClientHandlerContext.class);
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder securityPolicy(ClientSecurityPolicy securityPolicy) {
            context.setModule(NexalithicClient.MODULES.SecurityPolicy, securityPolicy);
            return this;
        }

        public NexalithicClient build() throws Throwable {
            return build(false);
        }
        public NexalithicClient build(boolean showOptions) throws Throwable {
            if (showOptions) {
                logger.trace("NexalithicClient-Options\n{}", OptionsDefinition.toString("com.nebilux.nexalithic", context));
            }

            controllerHandlerAssemblyBuilder.build().assembleInto(handlerRegistryBuilder);
            context.setModule(MODULES.EventBus, new NexalithicEventBus());
            context.setModule(HandlerCoordinator.MODULES.HandlerRegistry, handlerRegistryBuilder.build());
            context.setModule(BusinessPacketsAssembler.MODULES.PayloadRegistry, payloadRegistryBuilder.build());
            ClientHandlerCoordinator handlerCoordinator = new ClientHandlerCoordinator(context);
            context.setModule(MODULES.HandlerCoordinator, handlerCoordinator);
            context.setModule(MODULES.TaskScheduler, new TaskScheduler(context));
            context.setModule(MODULES.LinkStatusManager, new LinkStatusManager(context));
            context.setModule(ClientLifecycleManager.MODULES.SessionLoop, new ClientSessionLoop(context));
            context.setModule(MODULES.LifecycleManager, new ClientLifecycleManager(context));

            return new NexalithicClient(context);
        }
    }
}
