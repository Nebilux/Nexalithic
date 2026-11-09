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

package com.nebilux.nexalithic.core.io.codec;

import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.io.codec.assembler.BusinessPacketAssembler;
import com.nebilux.nexalithic.core.lifecycle.Lifecycle;
import com.nebilux.nexalithic.core.lifecycle.SharedLifecycle;
import com.nebilux.nexalithic.core.model.business.stream.codec.ChunkCodecRegistry;
import org.jctools.queues.MpmcArrayQueue;

import java.util.concurrent.CompletionStage;

/**
 * 共用的编解码资源。
 *
 * @author Reonvia
 * @since 0.4.0
 */
public class CodecResources {
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<ChunkCodecRegistry> ChunkCodecRegistry = defineModule(ChunkCodecRegistry.class);
        private Modules() {
            super(CodecResources.class);
        }
    }

    private final CodecSupplier codecSupplier;
    private final SharedLifecycle<TimeWheel<BusinessPacketAssembler>> assemblyWheel;

    public CodecResources(NexalithicBuilderContext context, NexalithicEndpoint.Modules modules) {
        assemblyWheel = context.getModule(BusinessPacketAssembler.MODULES.SharedTimeWheel,
                () -> new SharedLifecycle<>(new TimeWheel<>(
                        context.getOption(BusinessPacketAssembler.OPTIONS.TimeWheel.TickMillis),
                        context.getOption(BusinessPacketAssembler.OPTIONS.TimeWheel.SlotCount),
                        context.getOption(BusinessPacketAssembler.OPTIONS.TimeWheel.TickQuotaShift),
                        context.getOption(BusinessPacketAssembler.OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                        new GenericWrapperPool<>(
                                PoolStorageFactory.bounded(MpmcArrayQueue::new,
                                        context.getOption(BusinessPacketAssembler.OPTIONS.TimeWheel.WrapperPool_Capacity)),
                                PoolStrategyFactory.alwaysCreate(),
                                TimeWheel.ScheduleWrapper<BusinessPacketAssembler>::new
                        ),
                        BusinessPacketAssembler.class.getSimpleName()
                )));
        codecSupplier = new CodecSupplier(context, modules, assemblyWheel.component());
    }

    public CodecSupplier codecFactory() {
        return codecSupplier;
    }

    /** 在任何使用该资源的 Loop 启动前取得共享时间轮。 */
    public CompletionStage<Void> start() {
        return assemblyWheel.start(this);
    }

    /** 在所有使用该资源的 Loop 结束后，以对应关闭模式交还共享时间轮。 */
    public CompletionStage<Void> release(Lifecycle.ShutdownMode mode) {
        return assemblyWheel.release(this, mode);
    }
}
