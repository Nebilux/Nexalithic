package com.thezeroer.nexalithic.core.io.channel;

import com.thezeroer.nexalithic.core.io.loop.ChannelLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 循环通道
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/23
 */
public abstract class LoopChannel<L extends ChannelLoop<?, ?>, C extends SelectableChannel> extends AbstractChannel<C> {
    private static final Logger logger = LoggerFactory.getLogger(LoopChannel.class);
    private static final int DIRTY_BIT = 1 << 31; // 状态掩码：Bit 31 为 Dirty 位，低位存储 SelectionKey.OP_XXX
    private static final int INTEREST_MASK = ~DIRTY_BIT;
    private final AtomicInteger targetInterest = new AtomicInteger(0);
    protected final L ownerLoop;

    public LoopChannel(L ownerLoop) {
        this.ownerLoop = ownerLoop;
    }

    public final void updateInterest(int interest, boolean enable) {
        while (true) {
            int oldInterest = targetInterest.get();
            int newInterest = enable ? (oldInterest | interest) : (oldInterest & ~interest);
            if (newInterest == oldInterest && (oldInterest & DIRTY_BIT) != 0) {
                return;
            }
            if (!targetInterest.compareAndSet(oldInterest, newInterest | DIRTY_BIT)) {
                Thread.onSpinWait();
                continue;
            }
            if ((oldInterest & DIRTY_BIT) == 0) {
                if (ownerLoop.inEventLoop()) {
                    applyInterest();
                } else {
                    ownerLoop.submitInterestUpdate(this);
                }
            }
            return;
        }
    }
    public final void applyInterest() {
        if (!ownerLoop.inEventLoop()) {
            throw new IllegalStateException(
                    "Channel interest belongs to loop [%s], but was applied by [%s]"
                            .formatted(ownerLoop.getName(), Thread.currentThread().getName())
            );
        }
        while (true) {
            int oldInterest = targetInterest.get();
            if ((oldInterest & DIRTY_BIT) == 0) {
                return;
            }
            int newValue = oldInterest & INTEREST_MASK;
            if (targetInterest.compareAndSet(oldInterest, newValue)) {
                SelectionKey key = getSelectionKey();
                if (key != null) {
                    try {
                        if (key.interestOps() != newValue) {
                            key.interestOps(newValue);
                        }
                    } catch (IllegalArgumentException e) {
                        logger.error("applyTargetInterest error: {}", e.getMessage());
                    }
                }
                return;
            }
            Thread.onSpinWait();
        }
    }

    @Override
    protected void onOpen(Transport<C> transport) {
        targetInterest.set(transport.selectionKey().interestOps());
    }

    @Override
    protected final void checkOnLifecycle() {
        if (!ownerLoop.inEventLoop()) {
            throw new IllegalStateException(
                    "Channel lifecycle operation must be executed by owner loop [%s], current thread [%s]"
                            .formatted(ownerLoop.getName(), Thread.currentThread().getName())
            );
        }
    }
}
