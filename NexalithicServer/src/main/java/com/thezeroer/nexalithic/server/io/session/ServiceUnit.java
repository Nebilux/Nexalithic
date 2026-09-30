package com.thezeroer.nexalithic.server.io.session;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.loadbalance.LoadBalanceable;
import com.thezeroer.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.thezeroer.nexalithic.core.infra.loadbalance.P2CBalancer;
import com.thezeroer.nexalithic.server.io.session.business.BusinessLoop;
import com.thezeroer.nexalithic.server.io.session.signaling.SignalingLoop;

import java.io.IOException;

/**
 * 服务单元
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/02/18
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
