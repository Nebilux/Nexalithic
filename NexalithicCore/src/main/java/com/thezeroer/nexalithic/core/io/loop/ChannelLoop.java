package com.thezeroer.nexalithic.core.io.loop;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.session.channel.NexalithicChannel;
import com.thezeroer.nexalithic.core.session.channel.SessionChannel;
import org.jctools.queues.MpscUnboundedArrayQueue;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;

/**
 * 通道环路
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/08
 * @version 1.0.0
 */
public abstract class ChannelLoop<C extends NexalithicChannel> extends AbstractLoop {
    public static class Options extends AbstractLoop.Options {
        public final NexalithicOption<Integer> InterestQueue_ChunkSize = defineOption(
                1024, OptionValidator.positive()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
    }
    protected final MpscUnboundedArrayQueue<SessionChannel<?, ?>> interestQueue;

    public ChannelLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
        interestQueue = new MpscUnboundedArrayQueue<>(context.getOption(options.InterestQueue_ChunkSize));
    }

    public final void updateChannelInterest(SessionChannel<?, ?> channel) {
        interestQueue.offer(channel);
        wakeupIfNeeded();
    }

    public void postRateUpdate(SessionChannel<?, ?> channel) {}

    @Override
    protected final boolean asyncEvent() {
        interestQueue.drain(SessionChannel::applyTargetInterest);
        return onAsyncEvent() & interestQueue.isEmpty();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final void readyEvent(Selector selector) {
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            if (!key.isValid()) {
                keyNotValid(key);
                continue;
            }
            try {
                C channel = (C) key.attachment();
                channel.updateLastActiveTimeNanos(System.nanoTime());
                onReadyEvent(key, channel);
            } catch (Exception e) {
                logger.warn("[{}] failed to ready event: ", name, e);
                if (key.attachment() instanceof NexalithicChannel channel) {
                    closeChannel(channel);
                }
            }
        }
    }
    protected abstract void onReadyEvent(SelectionKey selectionKey, C channel);
    protected void keyNotValid(SelectionKey selectionKey) {
        if (selectionKey.attachment() instanceof NexalithicChannel channel) {
            closeChannel(channel);
        }
    }
    protected boolean closeChannel(NexalithicChannel channel) {
        if (channel.closeChannel()) {
            loadScore.decrement();
            return true;
        }
        return false;
    }

    @Override
    protected final void onReadyEvent(SelectionKey selectionKey) {}
}
