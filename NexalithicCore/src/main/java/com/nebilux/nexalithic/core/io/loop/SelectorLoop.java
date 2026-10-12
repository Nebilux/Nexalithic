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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import static com.nebilux.nexalithic.core.exception.FailureSupport.mergePrioritizingError;

/**
 * 选择器循环
 *
 * @author Reonvia
 * @since 0.2.0
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
    private record Constant(long SelectorTimeoutMillis) {}
    private final Constant CONSTANT;
    private final List<Runnable> preSelectTasks = new ArrayList<>(4);
    private final List<BooleanSupplier> immediateWorkConditions = new ArrayList<>(4);
    private final List<LongSupplier> selectWaitLimits = new ArrayList<>(2);
    private volatile Selector selector;
    private int prematureSelectReturns;

    public SelectorLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
        CONSTANT = context.getConstant(this.getClass(), Constant.class, () -> new Constant(
                context.getOption(options.SelectorTimeoutMillis)
        ));
        selector = Selector.open();

        registerDrainCondition(() -> {
            Selector current = selector;
            return current == null || current.keys().isEmpty();
        });
    }

    protected final void registerPreSelectTask(Runnable task) throws IllegalStateException {
        if (lifecycleState() != State.NEW) {
            throw new IllegalStateException(
                    "Pre-select tasks can only be registered before the loop starts"
            );
        }
        preSelectTasks.add(task);
    }

    /** 登记预选任务执行后仍需立即处理的工作，不应包含活动连接等关闭排空条件。 */
    protected final void registerImmediateWorkCondition(BooleanSupplier condition) {
        if (lifecycleState() != State.NEW) {
            throw new IllegalStateException("Immediate work conditions can only be registered before the loop starts");
        }
        immediateWorkConditions.add(Objects.requireNonNull(condition, "condition"));
    }

    /** 登记距离下一次预选任务运行所允许的最长等待时间，单位为纳秒。 */
    protected final void registerSelectWaitLimit(LongSupplier limitNanos) {
        if (lifecycleState() != State.NEW) {
            throw new IllegalStateException("Select wait limits can only be registered before the loop starts");
        }
        selectWaitLimits.add(Objects.requireNonNull(limitNanos, "limitNanos"));
    }

    protected final SelectionKey registerSelectableChannel(SelectableChannel channel, int interest) throws ClosedChannelException {
        Selector current = selector;
        if (current == null || !current.isOpen()) {
            throw new ClosedChannelException();
        }
        return channel.register(current, interest);
    }

    protected final Set<SelectionKey> registeredKeys() {
        Selector current = selector;
        return current == null ? Set.of() : current.keys();
    }

    @Override
    protected final void wakeupLoop() {
        Selector current = selector;
        if (current != null) {
            current.wakeup();
        }
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
                failure = mergePrioritizingError(failure, throwable);
            }
            for (SelectionKey key : current.keys().toArray(SelectionKey[]::new)) {
                try {
                    clearSelectionKey(key);
                } catch (Throwable throwable) {
                    failure = mergePrioritizingError(failure, throwable);
                }
            }
        } finally {
            selector = null;
            try {
                current.close();
            } catch (Throwable throwable) {
                failure = mergePrioritizingError(failure, throwable);
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
        long prematureSelectThresholdNanos = 0;
        int selected;
        try {
            runPreSelectTasks();
            if (hasImmediateWork()) {
                selected = current.selectNow();
            } else {
                long waitNanos = selectWaitNanos(draining, maxWaitNanos);
                long timeoutMillis = TimeUnit.NANOSECONDS.toMillis(waitNanos);
                if (!draining && waitNanos > 0 && timeoutMillis == 0) {
                    // 正常运行时允许至多晚 1 ms，避免在定时任务边界前反复空轮询。
                    timeoutMillis = 1;
                }
                if (timeoutMillis <= 0) {
                    selected = current.selectNow();
                } else {
                    normalBlockingSelect = !draining;
                    prematureSelectThresholdNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis) / 2;
                    long startedNanos = System.nanoTime();
                    selected = current.select(timeoutMillis);
                    elapsedNanos = System.nanoTime() - startedNanos;
                }
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
                && lifecycleState() == State.RUNNING
                && !current.keys().isEmpty()
                && elapsedNanos < prematureSelectThresholdNanos;
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
    protected void onSelectionKeyMigrated(SelectionKey oldKey, SelectionKey newKey) throws IOException {}
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

    private void runPreSelectTasks() {
        for (Runnable task : preSelectTasks) {
            task.run();
        }
    }

    private boolean hasImmediateWork() {
        for (BooleanSupplier condition : immediateWorkConditions) {
            if (condition.getAsBoolean()) {
                return true;
            }
        }
        return false;
    }

    private long selectWaitNanos(boolean draining, long maxWaitNanos) {
        long waitNanos = TimeUnit.MILLISECONDS.toNanos(CONSTANT.SelectorTimeoutMillis());
        if (draining) {
            waitNanos = Math.min(waitNanos, maxWaitNanos);
        }
        for (LongSupplier limit : selectWaitLimits) {
            waitNanos = Math.min(waitNanos, limit.getAsLong());
        }
        return waitNanos;
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
