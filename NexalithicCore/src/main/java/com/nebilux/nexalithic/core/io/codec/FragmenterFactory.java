package com.nebilux.nexalithic.core.io.codec;

import com.nebilux.nexalithic.core.NexalithicEndpoint;
import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStorageFactory;
import com.nebilux.nexalithic.core.infra.recyclable.PoolStrategyFactory;
import com.nebilux.nexalithic.core.infra.recyclable.WrapperPool;
import com.nebilux.nexalithic.core.io.codec.fragmenter.*;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpmcArrayQueue;

/**
 * 分片器工厂
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/10
 * @version 1.0.0
 */
public class FragmenterFactory {
    private final int SignalingPacketsFragmenter_WrapperQueue_Capacity_, BusinessPacketsFragmenter_WrapperQueue_Capacity_, WrapperLinked_Capacity_;
    private final WrapperPool<BusinessPacketFragmentWrapper> businessPacketWrapperPool;

    public FragmenterFactory(NexalithicBuilderContext context, NexalithicEndpoint.Modules modules) {
        SignalingPacketsFragmenter_WrapperQueue_Capacity_ = context.getOption(SignalingPacketsFragmenter.OPTIONS.WrapperQueue_Capacity);
        BusinessPacketsFragmenter_WrapperQueue_Capacity_ = context.getOption(BusinessPacketsFragmenter.OPTIONS.WrapperQueue_Capacity);
        WrapperLinked_Capacity_ = context.getOption(BusinessPacketsFragmenter.OPTIONS.WrapperLinked_Capacity);
        TaskScheduler taskScheduler = context.getModule(modules.TaskScheduler);
        //noinspection Convert2Diamond
        businessPacketWrapperPool = new GenericWrapperPool<BusinessPacket, BusinessPacketFragmentWrapper>(
                PoolStorageFactory.bounded(MpmcArrayQueue::new, context.getOption(BusinessPacketsFragmenter.OPTIONS.WrapperPool_Capacity)),
                PoolStrategyFactory.alwaysCreate(),
                owner -> new BusinessPacketFragmentWrapper(owner, new FragmentCallback(taskScheduler))
        );
    }

    public PacketsFragmenter<SignalingPacket> createSignaling() {
        return new SignalingPacketsFragmenter(SignalingPacketsFragmenter_WrapperQueue_Capacity_);
    }

    public PacketsFragmenter<BusinessPacket> createBusiness(NexalithicSession<?> session) {
        return new BusinessPacketsFragmenter(session, businessPacketWrapperPool,
                BusinessPacketsFragmenter_WrapperQueue_Capacity_, WrapperLinked_Capacity_);
    }
}
