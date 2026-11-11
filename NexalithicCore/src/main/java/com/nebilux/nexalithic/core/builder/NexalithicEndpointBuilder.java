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

package com.nebilux.nexalithic.core.builder;

import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;
import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;
import com.nebilux.nexalithic.core.messaging.handler.assembly.ControllerHandlerAssembler;
import com.nebilux.nexalithic.core.messaging.handler.assembly.ControllerHandlerAssemblerConfigurer;
import com.nebilux.nexalithic.core.messaging.handler.assembly.ControllerHandlerAssemblerHelper;
import com.nebilux.nexalithic.core.messaging.handler.mapping.HandlerRegistry;
import com.nebilux.nexalithic.core.messaging.handler.mapping.HandlerRegistryConfigurer;
import com.nebilux.nexalithic.core.model.business.codec.CodecRegistryHelper;
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;
import com.nebilux.nexalithic.core.model.business.packet.builtin.FilePart;
import com.nebilux.nexalithic.core.model.business.packet.builtin.TextPart;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodec;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodecRegistry;
import com.nebilux.nexalithic.core.model.business.packet.codec.PartCodecRegistryConfigurer;
import com.nebilux.nexalithic.core.model.business.stream.BusinessStream;
import com.nebilux.nexalithic.core.model.business.stream.builtin.FileChunk;
import com.nebilux.nexalithic.core.model.business.stream.builtin.MediaChunk;
import com.nebilux.nexalithic.core.model.business.stream.builtin.TextChunk;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodec;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodecRegistry;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodecRegistryConfigurer;

import java.util.List;

/**
 * Nexalithic 终端 Builder 抽象基类。
 *
 * <p>该类收敛 {@code NexalithicServer.Builder} 与 {@code NexalithicClient.Builder}
 * 共同的配置入口：通用选项、Payload 注册表、手写 Handler 注册表以及注解式 Controller
 * 装配器。具体终端 Builder 负责补充 Server/Client 专属配置，并在 {@code build()} 中创建
 * 对应的运行时模块。</p>
 *
 * <p>普通用户不会直接创建该抽象类，而是通过具体终端的 Builder 使用继承下来的方法：</p>
 *
 * <pre>{@code
 * NexalithicServer server = NexalithicServer.builder()
 *         .apply(SomeOptions.Enabled, true)
 *         .payloadCodecRegistryConfigurer((registry, helper) -> {
 *             registry.payloadCodec(new LoginPayload.Codec());
 *         })
 *         .controllerHandlerAssemblerConfigurer((assembler, helper) -> {
 *             helper.defaultHandlerMethodConverterSelector(assembler)
 *                     .controller(new UserController());
 *         })
 *         .build();
 * }</pre>
 *
 * <p>配置回调会在调用对应方法时立即作用到内部 Builder 上；最终的不可变注册表和 Handler
 * 映射由具体终端 Builder 在 {@code build()} 阶段统一构建。</p>
 *
 * <p>该 Builder 不是线程安全对象。应在单线程中完成配置，然后调用具体子类的
 * {@code build()} 方法创建终端实例。</p>
 *
 * @param <SELF> 具体 Builder 类型，用于保持链式调用返回子类类型
 * @param <HC> 终端 Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class NexalithicEndpointBuilder<SELF extends NexalithicEndpointBuilder<SELF, HC>, HC extends HandlerContext<?>> {
    /**
     * Handler 上下文类型。
     *
     * <p>由于 Java 泛型擦除，运行期无法直接从 {@code HC} 获得具体类型，所以由具体终端
     * Builder 在构造时显式传入。该类型会用于创建 Controller 装配辅助对象。</p>
     */
    protected final Class<HC> handlerContextType;

    /**
     * 当前构建过程共享的模块与选项上下文。
     *
     * <p>具体终端 Builder 会继续向该上下文写入 Server/Client 专属模块。</p>
     */
    protected final NexalithicBuilderContext context = new NexalithicBuilderContext();

    /**
     * {@link BusinessPacket.Part} 编解码器注册表构建器。
     *
     * <p>构造函数会预先注册内置编解码器。
     * 用户可通过 {@link #partCodecRegistryConfigurer(PartCodecRegistryConfigurer)} 追加自定义编解码器。</p>
     */
    protected final PartCodecRegistry.Builder partCodecRegistryBuilder = PartCodecRegistry.builder();

    /**
     * {@link BusinessStream.Chunk} 编解码器注册表构建器。
     *
     * <p>构造函数会预先注册内置编解码器。
     * 用户可通过 {@link #chunkCodecRegistryConfigurer(ChunkCodecRegistryConfigurer)} 追加自定义编解码器。</p>
     */
    protected final ChunkCodecRegistry.Builder chunkCodecRegistryBuilder = ChunkCodecRegistry.builder();

    /**
     * Handler 注册表构建器。
     *
     * <p>手动构建的 Handler 会直接注册到该构建器；注解式 Controller 会在具体终端的
     * {@code build()} 阶段被装配为 Handler 后再写入该构建器。</p>
     */
    protected final HandlerRegistry.Builder<HC> handlerRegistryBuilder = new HandlerRegistry.Builder<>();

    /**
     * Controller Handler 装配器构建器。
     *
     * <p>用于收集 Controller、拦截器和参数/返回值转换器等装配配置。</p>
     */
    protected final ControllerHandlerAssembler.Builder<HC> controllerHandlerAssemblyBuilder;

    /**
     * 创建终端 Builder 基类。
     *
     * @param handlerContextType 当前终端使用的 Handler 上下文类型
     */
    protected NexalithicEndpointBuilder(Class<HC> handlerContextType) {
        this.handlerContextType = handlerContextType;
        controllerHandlerAssemblyBuilder = ControllerHandlerAssembler.builder(handlerContextType);
        partCodecRegistryBuilder.codecs(List.of(
                new TextPart.Codec(),
                new FilePart.Codec()
        ));
        chunkCodecRegistryBuilder.codecs(List.of(
                new MediaChunk.Codec(),
                new FileChunk.Codec(),
                new TextChunk.Codec()
        ));
    }

    /**
     * 返回具体 Builder 自身。
     *
     * <p>子类通过实现该方法，让基类中的通用配置方法能够保持
     * {@code NexalithicServer.Builder} / {@code NexalithicClient.Builder} 的链式返回类型。</p>
     *
     * @return 当前具体 Builder 实例
     */
    protected abstract SELF self();

    /**
     * 设置 Nexalithic 通用选项或模块选项。
     *
     * <p>该方法适合配置框架内已经定义好的低层选项，例如线程数量、缓冲区大小、
     * 超时时间或模块运行参数。选项的可用范围由对应模块暴露的
     * {@link NexalithicOption} 决定。</p>
     *
     * <pre>{@code
     * NexalithicEndpoint.builder()
     *         .apply(LoopThread.OPTIONS.LocalLoopBufferPool_Capacity, 1024);
     * }</pre>
     *
     * @param option 要设置的选项定义
     * @param value 选项值
     * @param <T> 选项值类型
     * @return 当前具体 Builder，用于继续链式配置
     */
    public <T> SELF apply(NexalithicOption<T> option, T value) {
        context.setOption(option, value);
        return self();
    }

    /**
     * 配置 {@link PartCodecRegistry}
     *
     * <p>当业务需要传输自定义 {@link BusinessPacket.Part} 类型时，使用该入口注册对应的 {@link PartCodec}。
     * Builder 已默认注册 {@link TextPart}、{@link FilePart} 的编解码器。</p>
     *
     * @param configurer {@link PartCodecRegistry} 配置器
     * @return 当前具体 Builder，用于继续链式配置
     */
    public SELF partCodecRegistryConfigurer(PartCodecRegistryConfigurer configurer) {
        configure(PartCodecRegistry.class.getSimpleName(), () -> configurer.configure(partCodecRegistryBuilder, CodecRegistryHelper.INSTANCE));
        return self();
    }

    /**
     * 配置 {@link ChunkCodecRegistry}
     *
     * <p>当业务需要传输自定义 {@link BusinessStream.Chunk} 类型时，使用该入口注册对应的 {@link ChunkCodec}。
     * Builder 已默认注册 {@link MediaChunk}、{@link FileChunk}、{@link TextChunk} 的编解码器。</p>
     *
     * @param configurer {@link ChunkCodecRegistry} 配置器
     * @return 当前具体 Builder，用于继续链式配置
     */
    public SELF chunkCodecRegistryConfigurer(ChunkCodecRegistryConfigurer configurer) {
        configure(ChunkCodecRegistry.class.getSimpleName(), () -> configurer.configure(chunkCodecRegistryBuilder, CodecRegistryHelper.INSTANCE));
        return self();
    }

    /**
     * 配置 Handler 注册表。
     *
     * <p>当用户希望绕过注解式 Controller，直接注册
     * {@link com.nebilux.nexalithic.core.messaging.handler.NexalithicHandler} 或更换 Handler 存储形式时，使用该入口。
     * 如果使用的是注解式 Controller，通常应优先使用
     * {@link #controllerHandlerAssemblerConfigurer(ControllerHandlerAssemblerConfigurer)}。</p>
     *
     * @param configurer {@link HandlerRegistryConfigurer} Handler 注册表配置器
     * @return 当前具体 Builder，用于继续链式配置
     */
    public SELF handlerRegistryConfigurer(HandlerRegistryConfigurer<HC> configurer) {
        configure("handler registry", () -> configurer.configure(handlerRegistryBuilder, null));
        return self();
    }

    /**
     * 配置注解式 Controller Handler 装配器。
     *
     * <p>这是使用 Controller 编程模型时的主要入口。用户可以在这里注册 Controller 实例、
     * 配置拦截器、选择参数转换器以及返回值转换器。具体终端 Builder 会在 {@code build()}
     * 阶段调用装配器，把 Controller 方法转换为可执行 Handler 并写入 Handler 注册表。</p>
     *
     * @param configurer {@link ControllerHandlerAssemblerConfigurer} Controller Handler 装配器配置器
     * @return 当前具体 Builder，用于继续链式配置
     */
    public SELF controllerHandlerAssemblerConfigurer(ControllerHandlerAssemblerConfigurer<HC> configurer) {
        configure("controller handler assembler", () ->
                configurer.configure(controllerHandlerAssemblyBuilder, new ControllerHandlerAssemblerHelper<>(handlerContextType)));
        return self();
    }

    /** 在面向用户的 Builder 配置入口统一转换框架及回调失败。 */
    protected static void configure(String scope, Runnable action) {
        try {
            action.run();
        } catch (NexalithicUncheckedException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw NexalithicEndpointConfigurationException.forScope(scope, failure);
        }
    }
}
