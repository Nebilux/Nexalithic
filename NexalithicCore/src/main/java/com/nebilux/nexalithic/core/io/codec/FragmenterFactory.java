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
import com.nebilux.nexalithic.core.io.codec.fragmenter.*;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpmcArrayQueue;

/**
 * 分片器工厂
 *
 * @author Reonvia
 * @since 0.1.0
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
