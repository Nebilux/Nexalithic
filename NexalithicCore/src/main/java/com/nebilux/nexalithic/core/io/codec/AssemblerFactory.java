package com.nebilux.nexalithic.core.io.codec;

import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.nebilux.nexalithic.core.infra.recyclable.WrapperPool;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.io.codec.assembler.*;
import com.nebilux.nexalithic.core.messaging.payload.PayloadRegistry;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpmcArrayQueue;
import org.jctools.queues.SpscArrayQueue;

import java.util.concurrent.TimeUnit;

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

    public AssemblerFactory(NexalithicBuilderContext context, NexalithicEndpoint.Modules modules, NexalithicEndpoint.Type endpointType) {
        PacketQueue_Capacity_ = context.getOption(BusinessPacketsAssembler.OPTIONS.PacketQueue_Capacity);
        BusinessPacketAssemblyWrapper.Constant businessPacketAssemblyConstant = new BusinessPacketAssemblyWrapper.Constant(
                TimeUnit.NANOSECONDS.convert(context.getOption(BusinessPacketsAssembler.OPTIONS.MaxIdleTimeMillis), TimeUnit.MILLISECONDS)
        );
        PayloadRegistry payloadRegistry = context.getModule(BusinessPacketsAssembler.MODULES.PayloadRegistry);
        TaskScheduler taskScheduler = context.getModule(modules.TaskScheduler);
        //noinspection Convert2Diamond
        wrapperPool = new GenericWrapperPool<BusinessPacketAssemblyWrapper, BusinessPacketAssemblyWrapper>(
                PoolStorageFactory.bounded(
                        switch (endpointType) {
                            case CLIENT -> MpscArrayQueue::new;
                            case SERVER -> MpmcArrayQueue::new;
                        },
                        context.getOption(BusinessPacketsAssembler.OPTIONS.WrapperPool_Capacity)),
                PoolStrategyFactory.alwaysCreate(),
                owner -> new BusinessPacketAssemblyWrapper(owner, businessPacketAssemblyConstant, new AssemblyCallback(taskScheduler), payloadRegistry)
        );
        timeWheel = context.getModule(BusinessPacketsAssembler.MODULES.TimeWheel, () -> {
            TimeWheel<BusinessPacketAssemblyWrapper> timeWheel = new TimeWheel<>(
                    context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.TickMillis),
                    context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.SlotCount),
                    context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.TickQuotaShift),
                    context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.WaitQueue_ChunkSize),
                    new GenericWrapperPool<>(
                            PoolStorageFactory.bounded(
                                    switch (endpointType) {
                                        case CLIENT -> SpscArrayQueue::new;
                                        case SERVER -> SpmcArrayQueue::new;
                                    },
                                    context.getOption(BusinessPacketsAssembler.OPTIONS.TimeWheel.WrapperPool_Capacity)),
                            PoolStrategyFactory.alwaysCreate(),
                            TimeWheel.ScheduleWrapper<BusinessPacketAssemblyWrapper>::new
                    ),
                    BusinessPacketsAssembler.class.getSimpleName()
            );
            timeWheel.start();
            return timeWheel;
        });
    }

    public PacketsAssembler<SignalingPacket> createSignaling() {
        return new SignalingPacketsAssembler();
    }

    public PacketsAssembler<BusinessPacket> createBusiness(NexalithicSession<?> session) {
        return new BusinessPacketsAssembler(session, wrapperPool, timeWheel, PacketQueue_Capacity_);
    }
}
