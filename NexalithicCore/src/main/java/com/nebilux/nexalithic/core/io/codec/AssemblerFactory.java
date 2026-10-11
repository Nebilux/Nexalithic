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
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.nebilux.nexalithic.core.infra.recyclable.WrapperPool;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.io.codec.assembler.*;
import com.nebilux.nexalithic.core.lifecycle.Lifecycle;
import com.nebilux.nexalithic.core.lifecycle.SharedLifecycle;
import com.nebilux.nexalithic.core.messaging.payload.PayloadRegistry;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;

import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * 汇编器工厂
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class AssemblerFactory {
    private final WrapperPool<BusinessPacketAssemblyWrapper> wrapperPool;
    private final TimeWheel<BusinessPacketAssemblyWrapper> timeWheel;
    private final int PacketQueue_Capacity_;
    private final SharedLifecycle<TimeWheel<BusinessPacketAssemblyWrapper>> wheelLifecycle;

    public AssemblerFactory(NexalithicBuilderContext context, NexalithicEndpoint.Modules modules,
                            NexalithicEndpoint.Type endpointType) {
        PacketQueue_Capacity_ = context.getOption(BusinessPacketsAssembler.OPTIONS.PacketQueue_Capacity);
        BusinessPacketAssemblyWrapper.Constant businessPacketAssemblyConstant = new BusinessPacketAssemblyWrapper.Constant(
                TimeUnit.NANOSECONDS.convert(context.getOption(BusinessPacketsAssembler.OPTIONS.MaxIdleTimeMillis), TimeUnit.MILLISECONDS)
        );
        PayloadRegistry payloadRegistry = context.getModule(BusinessPacketsAssembler.MODULES.PayloadRegistry);
        TaskScheduler taskScheduler = context.getModule(modules.TaskScheduler);
        IntFunction<Queue<BusinessPacketAssemblyWrapper>> wrapperPoolQueueFactory = switch (endpointType) {
            case CLIENT -> MpscArrayQueue::new;
            case SERVER -> MpmcArrayQueue::new;
        };
        wrapperPool = new GenericWrapperPool<>(
                PoolStorageFactory.bounded(wrapperPoolQueueFactory, context.getOption(BusinessPacketsAssembler.OPTIONS.WrapperPool_Capacity)),
                PoolStrategyFactory.alwaysCreate(),
                owner -> new BusinessPacketAssemblyWrapper(owner, businessPacketAssemblyConstant, new AssemblyCallback(taskScheduler), payloadRegistry)
        );
        wheelLifecycle = context.getModule(BusinessPacketsAssembler.MODULES.SharedTimeWheel, () -> new SharedLifecycle<>(new TimeWheel<>(
                context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.TickMillis),
                context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.SlotCount),
                context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.TickQuotaShift),
                context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                new GenericWrapperPool<>(
                        PoolStorageFactory.bounded(
                                MpmcArrayQueue::new,
                                context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.WrapperPool_Capacity)),
                        PoolStrategyFactory.alwaysCreate(),
                        TimeWheel.ScheduleWrapper<BusinessPacketAssemblyWrapper>::new
                ),
                BusinessPacketsAssembler.class.getSimpleName()))
        );
        timeWheel = wheelLifecycle.component();
    }

    /** 所有使用此工厂的会话循环均结束后释放共享计时器。 */
    public CompletionStage<Void> release() {
        return wheelLifecycle.release(this, Lifecycle.ShutdownMode.ABORT);
    }

    /** 首次调用时启动共享计时器；构造工厂本身不会启动它。 */
    public CompletionStage<Void> start() {
        return wheelLifecycle.start(this);
    }

    public PacketsAssembler<SignalingPacket> createSignaling() {
        return new SignalingPacketsAssembler();
    }

    public PacketsAssembler<BusinessPacket> createBusiness(NexalithicSession<?> session) {
        return new BusinessPacketsAssembler(session, wrapperPool, timeWheel, PacketQueue_Capacity_);
    }
}
