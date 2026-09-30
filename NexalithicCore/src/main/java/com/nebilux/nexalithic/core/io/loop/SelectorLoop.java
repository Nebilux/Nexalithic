package com.nebilux.nexalithic.core.io.loop;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * 选择器循环
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/16
 */
public abstract class SelectorLoop extends AbstractLoop {
    public static class Options extends AbstractLoop.Options {
        public final NexalithicOption<Long> SelectorTimeoutMillis = defineOption(
                3_000L, OptionValidator.positive()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(SelectorLoop.class);
    private static final int MAX_PREMATURE_SELECT_RETURNS = 512;
    private record Constant(long SelectorTimeoutMillis, long PrematureSelectThresholdNanos) {
        private Constant(long selectorTimeoutMillis) {
            this(selectorTimeoutMillis, TimeUnit.MILLISECONDS.toNanos(selectorTimeoutMillis) / 2);
        }
    }
    private final Constant CONSTANT;
    private final List<BooleanSupplier> drainAsyncEventsConditions = new ArrayList<>(4);
    private volatile Selector selector;
    private int prematureSelectReturns;

    public SelectorLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
        CONSTANT = context.getConstant(this.getClass(), Constant.class, () -> new Constant(
                context.getOption(options.SelectorTimeoutMillis)
        ));
        selector = Selector.open();
        isDrainCondition(() -> selector.keys().isEmpty());
    }

    protected final SelectionKey registerSelectableChannel(SelectableChannel channel, int interest) throws ClosedChannelException {
        return channel.register(selector, interest);
    }
    protected final Set<SelectionKey> registeredKeys() {
        return selector.keys();
    }
    protected final void drainAsyncEventsCondition(BooleanSupplier condition) {
        if (getState() != State.NEW) {
            throw new IllegalStateException(
                    "drainAsyncEventsConditions can only be registered before the loop starts"
            );
        }
        drainAsyncEventsConditions.add(condition);
    }

    @Override
    protected final void wakeupLoop() {
        selector.wakeup();
    }

    @Override
    protected void sealLoop() {

    }

    @Override
    protected final void clearLoop() {
        Selector current = selector;
        if (current == null) {
            return;
        }
        Throwable failure = null;
        try {
            try {
                discardAsyncEvents();
            } catch (Throwable throwable) {
                failure = mergeThrowable(failure, throwable);
            }
            for (SelectionKey key : current.keys().toArray(SelectionKey[]::new)) {
                try {
                    clearSelectionKey(key);
                } catch (Throwable throwable) {
                    failure = mergeThrowable(failure, throwable);
                }
            }
        } finally {
            selector = null;
            try {
                current.close();
            } catch (Throwable throwable) {
                failure = mergeThrowable(failure, throwable);
            }
        }
        if (failure != null) {
            throw new IllegalStateException(
                    "Failed to clear selector loop [" + name + "]",
                    failure
            );
        }
    }

    @Override
    protected final void runIteration(boolean draining, long maxWaitNanos) throws Exception {
        Selector current = selector;
        boolean normalBlockingSelect = false;
        boolean explicitlyWoken;
        long elapsedNanos = 0;
        int selected;
        try {
            if (!drainAsyncEvents()) {
                selected = current.selectNow();
            } else if (!draining) {
                normalBlockingSelect = true;
                long startedNanos = System.nanoTime();
                selected = current.select(CONSTANT.SelectorTimeoutMillis);
                elapsedNanos = System.nanoTime() - startedNanos;
            } else {
                long timeoutMillis  = Math.min(CONSTANT.SelectorTimeoutMillis(), TimeUnit.NANOSECONDS.toMillis(maxWaitNanos));
                selected = timeoutMillis  == 0 ? current.selectNow() : current.select(timeoutMillis );
            }
        } finally {
            explicitlyWoken = acknowledgeWakeup();
        }
        Iterator<SelectionKey> iterator = current.selectedKeys().iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            try {
                onSelectorKeyReady(key);
            } catch (Exception failure) {
                logger.error("Selector Key {} Failed", key, failure);
                onSelectorKeyReadyFailed(key, failure);
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Selector loop thread interrupted");
        }
        boolean prematureReturn =
                normalBlockingSelect
                && selected == 0
                && !explicitlyWoken
                && getState() == State.RUNNING
                && !current.keys().isEmpty()
                && elapsedNanos < CONSTANT.PrematureSelectThresholdNanos();
        if (!prematureReturn) {
            prematureSelectReturns = 0;
            return;
        }
        if (++prematureSelectReturns >= MAX_PREMATURE_SELECT_RETURNS) {
            prematureSelectReturns = 0;
            logger.warn("Selector.select() returned prematurely {} times; rebuilding {}", MAX_PREMATURE_SELECT_RETURNS, current);
            rebuildSelector();
        }
    }

    protected abstract void onSelectorKeyReady(SelectionKey key) throws IOException;
    protected abstract void onSelectionKeyMigrated(SelectionKey oldKey, SelectionKey newKey) throws IOException;
    protected void onSelectorKeyReadyFailed(SelectionKey key, Exception exception) {
        key.cancel();
    }
    protected void onSelectorKeyMigratedFailed(SelectionKey oldKey, SelectionKey newKey, Exception exception) {
        if (newKey != null) {
            newKey.cancel();
        }
    }
    /**
     * 清理一个已经注册到当前 Selector 的 Key 及其关联资源。
     */
    protected abstract void clearSelectionKey(SelectionKey key) throws Exception;
    /**
     * 清理尚未注册到 Selector 或尚未执行的异步事件。
     */
    protected void discardAsyncEvents() {}

    private boolean drainAsyncEvents() {
        for (BooleanSupplier condition : drainAsyncEventsConditions) {
            if (!condition.getAsBoolean()) {
                return false;
            }
        }
        return true;
    }
    private void rebuildSelector() throws IOException {
        Selector oldSelector = selector;
        Selector newSelector = oldSelector.provider().openSelector();
        for (SelectionKey oldKey : oldSelector.keys()) {
            if (!oldKey.isValid()) {
                continue;
            }
            SelectionKey newKey = null;
            try {
                newKey = oldKey.channel().register(newSelector, oldKey.interestOps(), oldKey.attachment());
                onSelectionKeyMigrated(oldKey, newKey);
            } catch (Exception failure) {
                logger.error("Selector Key {} Failed", oldKey, failure);
                onSelectorKeyMigratedFailed(oldKey, newKey, failure);
            } finally {
                oldKey.cancel();
            }
        }
        selector = newSelector;
        newSelector.wakeup();
        try {
            oldSelector.close();
        } catch (IOException failure) {
            logger.error("Failed to close replaced Selector", failure);
        }
    }
}
