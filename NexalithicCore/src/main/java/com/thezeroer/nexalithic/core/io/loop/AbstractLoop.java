package com.thezeroer.nexalithic.core.io.loop;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.loadbalance.LoadBalanceable;
import com.thezeroer.nexalithic.core.io.thread.LoopThread;
import com.thezeroer.nexalithic.core.session.channel.NexalithicChannel;
import com.thezeroer.nexalithic.core.session.channel.SessionChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * 抽象循环
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/06
 * @version 1.0.0
 */
public abstract class AbstractLoop implements LoadBalanceable, Runnable {
    public static class Options extends OptionsDefinition {
        public final NexalithicOption<Long> Shutdown_MaxWaitTime = defineOption(
                300000L, OptionValidator.positive()
        );
        public final NexalithicOption<Long> Selector_Timeout = defineOption(
                3000L, OptionValidator.nonNegative()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
    }
    public record Constant(long Shutdown_MaxWaitTime, long Selector_Timeout) {}
    private final Constant CONSTANT;
    protected enum State {
        NEW,
        STARTING,
        WORKING,
        WAITING,
        STOPPING,
        SHUTTING_DOWN,
        TERMINATED
    }
    protected static final Logger logger = LoggerFactory.getLogger(AbstractLoop.class);
    protected static final int MAX_EPOLL = 512;
    protected final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    protected final LongAdder loadScore = new LongAdder();
    protected volatile Selector selector;

    protected final LoopThread thread;
    protected volatile String name = getClass().getSimpleName();

    public AbstractLoop(NexalithicBuilderContext context, Options options) throws IOException {
        CONSTANT = context.getConstant(this.getClass(), Constant.class, () -> new Constant(
                context.getOption(options.Shutdown_MaxWaitTime),
                context.getOption(options.Selector_Timeout)
        ));
        selector = Selector.open();
        thread = new LoopThread(context, this);
        thread.setDaemon(false);
    }

    public final void start() {
        synchronized (this) {
            if (!state.compareAndSet(State.NEW, State.STARTING)) {
                throw new IllegalStateException("Loop already [%s]".formatted(state.get()));
            }
            thread.setName(name);
            thread.start();
        }
    }

    public final void stop() {
        synchronized (this) {
            if (state.get() == State.STOPPING || state.get() == State.TERMINATED) {
                throw new IllegalStateException("Loop already [%s]".formatted(state.get()));
            }
            state.set(State.STOPPING);
            selector.wakeup();
        }
    }

    public final void shutdown() {
        synchronized (this) {
            if (state.get() == State.SHUTTING_DOWN || state.get() == State.STOPPING || state.get() == State.TERMINATED) {
                throw new IllegalStateException("Loop already [%s]".formatted(state.get()));
            }
            state.set(State.SHUTTING_DOWN);
            selector.wakeup();
        }
    }

    /**
     * 需要时叫醒，只有当 Selector 真的在睡觉（WAITING）时，才将其叫醒并设为 WORKING
     *
     */
    public final void wakeupIfNeeded() {
        if (state.compareAndSet(State.WAITING, State.WORKING)) {
            selector.wakeup();
        }
    }

    @Override
    public final long getLoadScore() {
        return loadScore.sum();
    }

    public final String getName() {
        return name;
    }
    public final AbstractLoop addIdToName(String id) {
        name = name + "-" + id;
        return this;
    }

    protected abstract boolean onAsyncEvent();
    protected abstract void onReadyEvent(SelectionKey selectionKey) throws IOException;
    protected void onShuttingDown() {}
    protected void onTerminated() {}
    protected void onSelectorError(IOException error) {}

    @Override
    public final void run() {
        Selector localSelector = this.selector;
        int emptyCount = 0, readyCount;
        long start = 0, end = 0;
        boolean running = true;
        state.compareAndSet(State.STARTING, State.WORKING);
        logger.debug("[{}] started", name);
        while (running) {
            switch (state.get()) {
                case WORKING -> {
                    try {
                        if (asyncEvent()) {
                            if (state.compareAndSet(State.WORKING, State.WAITING)) {
                                start = System.currentTimeMillis();
                                // 再次检查 asyncEvent，防止在 CAS 之后、select 之前有新任务进来
                                if (!onAsyncEvent()) {
                                    state.set(State.WORKING);
                                    readyCount = localSelector.selectNow();
                                } else {
                                    readyCount = localSelector.select(CONSTANT.Selector_Timeout);
                                }
                                state.compareAndSet(State.WAITING, State.WORKING);
                                end = System.currentTimeMillis();
                            } else {
                                continue;
                            }
                        } else {
                            readyCount = localSelector.selectNow();
                        }
                        if (readyCount > 0) {
                            emptyCount = 0;
                            readyEvent(localSelector);
                        } else if (start != 0) {
                            if (end - start < CONSTANT.Selector_Timeout / 2) {
                                if (++emptyCount > MAX_EPOLL) {
                                    logger.warn("[{}] Epoll bug detected, rebuilding selector...", name);
                                    localSelector = rebuildSelector();
                                    emptyCount = 0;
                                }
                            } else {
                                emptyCount = 0;
                            }
                            start = 0;
                            end = 0;
                        }
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                        onSelectorError(e);
                    }
                }
                case SHUTTING_DOWN -> {
                    if (start == 0) {
                        onShuttingDown();
                        start = System.currentTimeMillis();
                    }
                    try {
                        if (localSelector.selectNow() > 0) {
                            readyEvent(localSelector);
                        }
                    } catch (IOException ignored) {}
                    if (localSelector.keys().isEmpty()) {
                        try {
                            localSelector.close();
                        } catch (IOException e) {
                            logger.error("[{}] Error closing loop", name, e);
                        }
                        logger.debug("[{}] shutdown", name);
                    } else if (System.currentTimeMillis() - start > CONSTANT.Shutdown_MaxWaitTime) {
                        logger.warn("[{}] max shutdown time exceeded, forcing stop.", name);
                        state.set(State.STOPPING);
                    }
                }
                case STOPPING -> {
                    for (SelectionKey key : localSelector.keys()) {
                        try {
                            key.channel().close();
                        } catch (IOException ignored) {}
                    }
                    try {
                        localSelector.close();
                    } catch (IOException e) {
                        logger.error("[{}] Error closing loop", name, e);
                    }
                    logger.debug("[{}] stopped", name);
                    running = false;
                }
                default -> running = false;
            }
        }
        onTerminated();
        state.set(State.TERMINATED);
    }
    protected boolean asyncEvent() {
        return onAsyncEvent();
    }
    protected void readyEvent(Selector selector) throws IOException {
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            try {
                onReadyEvent(key);
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] failed to ready event: ", name, e);
                }
                if (key.attachment() instanceof NexalithicChannel channel) {
                    if (channel.closeChannel()) {
                        loadScore.decrement();
                    }
                }
            }
        }
    }
    protected final Selector rebuildSelector() throws IOException {
        try {
            Selector newSelector = Selector.open();
            Selector oldSelector = this.selector;
            for (SelectionKey oldKey : oldSelector.keys()) {
                if (!oldKey.isValid()) continue;
                Object attachment = oldKey.attachment();
                try {
                    SelectionKey newKey = oldKey.channel().register(newSelector, oldKey.interestOps(), attachment);
                    if (attachment instanceof SessionChannel<?, ?> sessionChannel) {
                        sessionChannel.updateChannel(newKey);
                    }
                } catch (Exception e) {
                    logger.error("[{}] Failed to migrate key for channel", name, e);
                }
            }
            this.selector = newSelector;
            oldSelector.close();
            logger.info("[{}] Selector rebuild successfully completed", name);
            return this.selector;
        } catch (IOException e) {
            logger.error("[{}] Failed to rebuild selector", name, e);
            throw e;
        }
    }
}
