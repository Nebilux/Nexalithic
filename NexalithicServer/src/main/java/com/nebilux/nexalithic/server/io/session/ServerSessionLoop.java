package com.nebilux.nexalithic.server.io.session;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.infra.timer.TimerContext;
import com.nebilux.nexalithic.core.infra.timer.TimerCoordinator;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.session.SessionChannel;
import com.nebilux.nexalithic.server.io.handshake.HandshakeContext;
import com.nebilux.nexalithic.server.session.ServerSession;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 服务器会话循环
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class ServerSessionLoop<P extends AbstractPacket> extends SessionLoop<HandshakeContext, SessionChannel<P, ServerSession>> implements TimerCoordinator<SessionChannel<P, ServerSession>> {
    public static abstract class Options extends SessionLoop.Options {
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
    protected record Constant(int DispatchQueue_DrainLimit, long MaxIdleTimeNanos) {}
    protected final Constant CONSTANT;

    public ServerSessionLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
        CONSTANT = context.getConstant(this.getClass(), Constant.class, () -> new Constant(
                context.getOption(options.DispatchQueue_DrainLimit),
                TimeUnit.MILLISECONDS.toNanos(context.getOption(options.MaxIdleTimeMillis))
        ));
    }

    @Override
    protected boolean onExecuteReleaseEvent(SessionChannel<P, ServerSession> channel) {
        return true;
    }

    @Override
    public long getExpiryTimeNanos(TimerContext<SessionChannel<P, ServerSession>> context) {
        return getChannelExpiryTimeNanos(context.target());
    }

    @Override
    public boolean isCancelled(TimerContext<SessionChannel<P, ServerSession>> context) {
        return context.target().getLastActiveTimeNanos() == -1;
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<SessionChannel<P, ServerSession>> context) {
        SessionChannel<P, ServerSession> target = context.target();
        if (System.nanoTime() < getChannelExpiryTimeNanos(target)) {
            return false;
        }
        submitDisconnectEvent(target, Event.Disconnect.Reason.IDLE_TIMEOUT);
        return true;
    }

    protected long getChannelExpiryTimeNanos(SessionChannel<P, ServerSession> channel) {
        return channel.getLastActiveTimeNanos() + CONSTANT.MaxIdleTimeNanos();
    }
}
