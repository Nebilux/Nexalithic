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

package com.nebilux.nexalithic.client;

import com.nebilux.nexalithic.client.exception.NexalithicClientLinkException;
import com.nebilux.nexalithic.client.io.session.ClientSessionLoop;
import com.nebilux.nexalithic.client.lifecycle.ClientLifecycleCoordinator;
import com.nebilux.nexalithic.client.messaging.ClientHandlerContext;
import com.nebilux.nexalithic.client.messaging.ClientHandlerCoordinator;
import com.nebilux.nexalithic.client.security.ClientSecurityPolicy;
import com.nebilux.nexalithic.client.session.ClientSession;
import com.nebilux.nexalithic.client.session.SessionManager;
import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.NexalithicEndpointBuildException;
import com.nebilux.nexalithic.core.builder.NexalithicEndpointBuilder;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;
import com.nebilux.nexalithic.core.io.codec.assembler.BusinessPacketsAssembler;
import com.nebilux.nexalithic.core.messaging.handler.HandlerCoordinator;
import com.nebilux.nexalithic.core.messaging.task.NexalithicTask;
import com.nebilux.nexalithic.core.messaging.task.TaskHandle;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.NexalithicSessionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.concurrent.locks.LockSupport;

/**
 * Nexalithic客户端
 *
 * @author Reonvia
 * @since 0.1.0
 */
@SuppressWarnings("UnusedReturnValue")
public class NexalithicClient extends NexalithicEndpoint {
    public static final Modules MODULES = new Modules();
    public static final class Modules extends NexalithicEndpoint.Modules {
        public final NexalithicModule<SessionManager> SessionManager = defineModule(SessionManager.class);
        private Modules() {
            super(NexalithicClient.class);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NexalithicClient.class);
    private final SessionManager sessionManager;

    private NexalithicClient(NexalithicBuilderContext context) {
        super(context, MODULES);
        this.sessionManager = context.getModule(MODULES.SessionManager);
        System.gc();
    }
    public static NexalithicClient unsafeCreate(NexalithicBuilderContext context) {
        return new NexalithicClient(context);
    }

    public static Builder builder() {
        logger.info(Banner.render(NexalithicClient.class));
        return new Builder();
    }

    /**
     * 建立信令连接并完成安全握手。
     *
     * @return 连接结果已提交给会话循环时返回 {@code true}；被拒绝时返回 {@code false}
     * @throws NexalithicClientLinkException 网络连接或安全握手失败
     */
    public boolean link(InetSocketAddress remote) throws NexalithicClientLinkException {
        try {
            return sessionManager.connect(remote);
        } catch (IOException | GeneralSecurityException | SecurityException exception) {
            throw NexalithicClientLinkException.forRemote(remote, exception);
        }
    }
    public void unlink() {
        sessionManager.disconnect();
    }

    public TaskHandle submit(NexalithicTask.Builder taskBuilder) {
        return requireSession().getTaskCoordinator().submit(taskBuilder);
    }
    public boolean push(BusinessPacket packet) {
        return requireSession().pushBusinessPacket(packet);
    }

    public SessionManager.State getSessionState() {
        return sessionManager.getState();
    }

    private ClientSession getSession() {
        if (sessionManager.isShuttingDown()) {
            return null;
        }
        ClientSession session = sessionManager.getCurrentSession();
        if (session == null) {
            for (int i = 0; i < 100; i++) {
                if (sessionManager.isShuttingDown()) {
                    return null;
                }
                if (session != null) {
                    return session;
                } else {
                    if (i < 50) {
                        Thread.onSpinWait();
                    } else {
                        LockSupport.parkNanos(i * 1_000_000L);
                    }
                }
                session = sessionManager.getCurrentSession();
            }
        }
        return session;
    }

    private ClientSession requireSession() {
        ClientSession session = getSession();
        if (session == null) {
            throw NexalithicSessionException.noActiveClientSession();
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
            configure("client security policy", () -> context.setModule(NexalithicClient.MODULES.SecurityPolicy, securityPolicy));
            return this;
        }

        public NexalithicClient build() throws NexalithicEndpointBuildException {
            return build(false);
        }
        public NexalithicClient build(boolean showOptions) throws NexalithicEndpointBuildException {
            try {
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
                SessionManager sessionManager = new SessionManager(context);
                context.setModule(MODULES.SessionManager, sessionManager);
                ClientSessionLoop sessionLoop = new ClientSessionLoop(context);
                context.setModule(ClientLifecycleCoordinator.MODULES.SessionLoop, sessionLoop);
                sessionManager.bindSessionLoop(sessionLoop);
                context.setModule(MODULES.Lifecycle, new ClientLifecycleCoordinator(context));

                return new NexalithicClient(context);
            } catch (Error | NexalithicUncheckedException failure) {
                throw failure;
            } catch (Throwable failure) {
                throw NexalithicEndpointBuildException.forTarget(NexalithicClient.class, failure);
            }
        }
    }
}
