package com.thezeroer.nexalithic.server.lifecycle.service;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.loadbalance.LoadBalanceable;
import com.thezeroer.nexalithic.core.infra.loadbalance.LoadBalancer;
import com.thezeroer.nexalithic.core.infra.loadbalance.P2CBalancer;
import com.thezeroer.nexalithic.core.session.SessionAttachment;

import java.io.IOException;

/**
 * 服务单元
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/02/18
 */
public class ServiceUnit implements LoadBalanceable, SessionAttachment {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, ServiceUnit.class);
    public static final class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> WorkerLoop_Count = defineOption(
                Runtime.getRuntime().availableProcessors(), OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    private final StewardLoop stewardLoop;
    private final WorkerLoop[] workerLoops;
    private final LoadBalancer<Void, WorkerLoop> workerLoopBalancer;

    public ServiceUnit(NexalithicBuilderContext context) throws IOException {
        stewardLoop = new StewardLoop(context, this);
        workerLoops = new WorkerLoop[context.getOption(OPTIONS.WorkerLoop_Count)];
        for (int i = 0; i < workerLoops.length; i++) {
            workerLoops[i] = new WorkerLoop(context);
        }
        workerLoopBalancer = new P2CBalancer<>(workerLoops);
    }

    public StewardLoop getStewardLoop() {
        return stewardLoop;
    }
    public WorkerLoop selectWorkerLoop() {
        return workerLoopBalancer.select(null);
    }
    public WorkerLoop[] getWorkerLoops() {
        return workerLoops;
    }

    public ServiceUnit addIdToLoopName(String id) {
        stewardLoop.addIdToName(id);
        for (int i = 0; i < workerLoops.length; i++) {
            workerLoops[i].addIdToName(id + "-" + i);
        }
        return this;
    }

    @Override
    public long getLoadScore() {
        return stewardLoop.getLoadScore();
    }

    @Override
    public void clear() {
    }
}
