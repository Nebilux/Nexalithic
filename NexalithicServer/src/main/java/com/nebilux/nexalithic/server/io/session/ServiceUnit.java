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

package com.nebilux.nexalithic.server.io.session;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalanceable;
import com.nebilux.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.nebilux.nexalithic.core.infra.loadbalance.P2CBalancer;
import com.nebilux.nexalithic.server.io.session.business.BusinessLoop;
import com.nebilux.nexalithic.server.io.session.signaling.SignalingLoop;

import java.io.IOException;

/**
 * 服务单元
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ServiceUnit implements LoadBalanceable {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, ServiceUnit.class);
    public static final class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> BusinessLoop_Count = defineOption(
                Runtime.getRuntime().availableProcessors(), OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    private final SignalingLoop signalingLoop;
    private final BusinessLoop[] businessLoops;
    private final LoadBalancer<Void, BusinessLoop> businessBalancer;

    public ServiceUnit(NexalithicBuilderContext context) throws IOException {
        signalingLoop = new SignalingLoop(context, this);
        businessLoops = new BusinessLoop[context.getOption(OPTIONS.BusinessLoop_Count)];
        for (int i = 0; i < businessLoops.length; i++) {
            businessLoops[i] = new BusinessLoop(context);
        }
        businessBalancer = new P2CBalancer<>(businessLoops);
    }

    public SignalingLoop getSignalingLoop() {
        return signalingLoop;
    }
    public BusinessLoop selectBusinessLoop() {
        return businessBalancer.select(null);
    }
    public BusinessLoop[] getBusinessLoops() {
        return businessLoops;
    }

    @Override
    public long getLoadScore() {
        return signalingLoop.getLoadScore();
    }
}
