package com.thezeroer.nexalithic.server.lifecycle.service;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.io.loop.ChannelLoop;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.server.lifecycle.handshake.PendingChannel;
import com.thezeroer.nexalithic.server.lifecycle.service.session.ServerSessionChannel;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;

/**
 * 服务 Loop
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/08
 * @version 1.0.0
 */
public abstract class ServiceLoop<P extends AbstractPacket> extends ChannelLoop<ServerSessionChannel<P>> {
    public static class Options extends ChannelLoop.Options {
        public final NexalithicOption<Integer> DispatchQueue_Capacity = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> DispatchQueue_DrainLimit = defineOption(
                256, OptionValidator.positive()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
    }
    protected record Constant(int DrainLimit) {}
    protected final Constant CONSTANT;
    protected final MpscArrayQueue<PendingChannel> dispatchQueue;

    public ServiceLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
        CONSTANT = context.getConstant(this.getClass(), Constant.class, () -> new Constant(
                context.getOption(options.DispatchQueue_DrainLimit)
        ));
        this.dispatchQueue = new MpscArrayQueue<>(context.getOption(options.DispatchQueue_Capacity));
    }

    public final void dispatch(PendingChannel pendingChannel) {
        if (dispatchQueue.offer(pendingChannel)) {
            loadScore.increment();
            wakeupIfNeeded();
        } else {
            pendingChannel.closeChannel();
        }
    }
}
