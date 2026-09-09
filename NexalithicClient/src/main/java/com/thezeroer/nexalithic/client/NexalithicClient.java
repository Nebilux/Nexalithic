package com.thezeroer.nexalithic.client;

import com.thezeroer.nexalithic.client.lifecycle.ClientLifecycleManager;
import com.thezeroer.nexalithic.client.lifecycle.GeneralLoop;
import com.thezeroer.nexalithic.client.lifecycle.session.ClientSession;
import com.thezeroer.nexalithic.client.manager.LinkStatusManager;
import com.thezeroer.nexalithic.client.messaging.ClientHandlerContext;
import com.thezeroer.nexalithic.client.messaging.ClientHandlerCoordinator;
import com.thezeroer.nexalithic.client.security.ClientSecurityPolicy;
import com.thezeroer.nexalithic.core.NexalithicEndpoint;
import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.NexalithicEndpointBuilder;
import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.event.NexalithicEventBus;
import com.thezeroer.nexalithic.core.io.codec.assembler.BusinessPacketsAssembler;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.thezeroer.nexalithic.core.messaging.task.NexalithicTask;
import com.thezeroer.nexalithic.core.messaging.task.TaskHandle;
import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
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
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, NexalithicClient.class);
    public static final class Options extends NexalithicEndpoint.Options {
        Options(Class<?> holder) {
            super(holder);
        }
        @Override
        protected Type EndpointType_Value() {
            return Type.CLIENT;
        }
    }
    public static final class Modules extends NexalithicEndpoint.Modules {
        public static final NexalithicModule<LinkStatusManager> LinkStatusManager = NexalithicModule.create("NexalithicClient_LinkStatusManager", LinkStatusManager.class);
    }

    private static final Logger logger = LoggerFactory.getLogger(NexalithicClient.class);
    private final LinkStatusManager linkStatusManager;
    private final GeneralLoop generalLoop;

    private NexalithicClient(NexalithicBuilderContext context) {
        super(context.getModule(Modules.LifecycleManager), context.getModule(Modules.EventBus));
        this.linkStatusManager = context.getModule(Modules.LinkStatusManager);
        this.generalLoop = context.getModule(ClientLifecycleManager.Modules.GeneralLoop);
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
        generalLoop.getNetworkRouter().setServerAddress(remote);
        try {
            if (generalLoop.link(AbstractPacket.PacketType.SIGNALING, socketChannel, null)) {
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
        generalLoop.unlink();
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
        ClientSession session = generalLoop.getSession();
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
                session = generalLoop.getSession();
            }
        }
        return session;
    }

    public static class Builder extends NexalithicEndpointBuilder<Builder, ClientHandlerContext> {
        public Builder() {
            super(ClientHandlerContext.class, OPTIONS);
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder securityPolicy(ClientSecurityPolicy securityPolicy) {
            context.setModule(Modules.SecurityPolicy, securityPolicy);
            return this;
        }

        public NexalithicClient build() throws Throwable {
            return build(false);
        }
        public NexalithicClient build(boolean showOptions) throws Throwable {
            if (showOptions) {
                logger.trace("NexalithicClient-Options\n{}", OptionsDefinition.toString("com.thezeroer.nexalithic", context));
            }

            controllerHandlerAssemblyBuilder.build().assembleInto(handlerRegistryBuilder);
            context.setModule(Modules.EventBus, new NexalithicEventBus());
            context.setModule(HandlerCoordinator.Modules.HandlerRegistry, handlerRegistryBuilder.build());
            context.setModule(BusinessPacketsAssembler.Modules.PayloadRegistry, payloadRegistryBuilder.build());
            ClientHandlerCoordinator handlerCoordinator = new ClientHandlerCoordinator(context);
            context.setModule(Modules.HandlerCoordinator, handlerCoordinator);
            context.setModule(Modules.TaskScheduler, new TaskScheduler(context));
            context.setModule(Modules.LinkStatusManager, new LinkStatusManager(context));
            context.setModule(ClientLifecycleManager.Modules.GeneralLoop, new GeneralLoop(context));
            context.setModule(Modules.LifecycleManager, new ClientLifecycleManager(context));

            return new NexalithicClient(context);
        }
    }
}
