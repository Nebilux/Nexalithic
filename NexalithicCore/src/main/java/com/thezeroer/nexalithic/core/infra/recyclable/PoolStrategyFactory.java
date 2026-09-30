package com.thezeroer.nexalithic.core.infra.recyclable;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 池资源获取策略工厂。
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/19
 */
public final class PoolStrategyFactory {
    private PoolStrategyFactory() {}

    /**
     * 池未命中时总是创建，不限制活动资源数量。
     */
    public static PoolStrategy alwaysCreate() {
        return StatelessStrategy.ALWAYS_CREATE;
    }

    /**
     * 池未命中时返回 {@code null}。
     */
    public static PoolStrategy skip() {
        return StatelessStrategy.SKIP;
    }

    /**
     * 池未命中时抛出异常。
     */
    public static PoolStrategy fail() {
        return StatelessStrategy.FAIL_ON_MISS;
    }

    /**
     * 达到活动资源上限时立即抛出异常。
     */
    public static PoolStrategy failFast(int maxActive) {
        return new FailFastStrategy(maxActive);
    }

    /**
     * 达到活动资源上限时等待，直到有配额被归还。
     */
    public static PoolStrategy blocking(int maxActive) {
        return new BlockingStrategy(maxActive);
    }

    /**
     * 在指定时间内等待配额，超时后借用配额并继续创建或复用资源。
     */
    public static PoolStrategy waitOrCreate(int maxActive, long timeoutMillis) {
        return new WaitOrCreateStrategy(maxActive, timeoutMillis);
    }

    /**
     * 在指定时间内等待配额，超时后抛出异常。
     */
    public static PoolStrategy waitOrFail(int maxActive, long timeoutMillis) {
        return new WaitOrFailStrategy(maxActive, timeoutMillis);
    }

    private enum StatelessStrategy implements PoolStrategy {
        ALWAYS_CREATE {
            @Override
            public boolean allowCreate() {
                return true;
            }
        },
        SKIP {
            @Override
            public boolean allowCreate() {
                return false;
            }
        },
        FAIL_ON_MISS {
            @Override
            public boolean allowCreate() {
                throw new IllegalStateException("Pool storage is empty");
            }
        };

        @Override
        public Permit beforeAcquire() {
            return Permit.NONE;
        }

        @Override
        public void afterRelease(Permit permit) {
            if (permit != Permit.NONE) {
                throw new IllegalStateException("Unexpected permit: " + permit);
            }
        }
    }

    private abstract static class SemaphoreStrategy implements PoolStrategy {
        private final Semaphore semaphore;

        private SemaphoreStrategy(int maxActive) {
            if (maxActive <= 0) {
                throw new IllegalArgumentException("maxActive must be greater than zero");
            }
            this.semaphore = new Semaphore(maxActive);
        }

        @Override
        public boolean allowCreate() {
            return true;
        }

        @Override
        public void afterRelease(Permit permit) {
            if (permit != Permit.ACQUIRED) {
                throw new IllegalStateException("Unexpected permit: " + permit);
            }
            semaphore.release();
        }

        protected final boolean tryAcquire() {
            return semaphore.tryAcquire();
        }

        protected final void acquire() throws InterruptedException {
            semaphore.acquire();
        }

        protected final boolean tryAcquire(long timeoutMillis) throws InterruptedException {
            return semaphore.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        protected final IllegalStateException interrupted(InterruptedException cause) {
            Thread.currentThread().interrupt();
            return new IllegalStateException("Interrupted while waiting for pool permit", cause);
        }

        protected final IllegalStateException exhausted(String message) {
            return new IllegalStateException(message + " [available=" + semaphore.availablePermits() + ']');
        }
    }

    private static final class FailFastStrategy extends SemaphoreStrategy {
        private FailFastStrategy(int maxActive) {
            super(maxActive);
        }

        @Override
        public Permit beforeAcquire() {
            if (!tryAcquire()) {
                throw exhausted("Pool active limit reached");
            }
            return Permit.ACQUIRED;
        }
    }

    private static final class BlockingStrategy extends SemaphoreStrategy {
        private BlockingStrategy(int maxActive) {
            super(maxActive);
        }

        @Override
        public Permit beforeAcquire() {
            try {
                acquire();
                return Permit.ACQUIRED;
            } catch (InterruptedException e) {
                throw interrupted(e);
            }
        }
    }

    private abstract static class TimedSemaphoreStrategy extends SemaphoreStrategy {
        private final long timeoutMillis;

        private TimedSemaphoreStrategy(int maxActive, long timeoutMillis) {
            super(maxActive);
            if (timeoutMillis < 0) {
                throw new IllegalArgumentException("timeoutMillis must not be negative");
            }
            this.timeoutMillis = timeoutMillis;
        }

        protected final boolean tryAcquireBeforeTimeout() throws InterruptedException {
            return tryAcquire(timeoutMillis);
        }
    }

    private static final class WaitOrCreateStrategy extends TimedSemaphoreStrategy {
        private WaitOrCreateStrategy(int maxActive, long timeoutMillis) {
            super(maxActive, timeoutMillis);
        }

        @Override
        public Permit beforeAcquire() {
            try {
                return tryAcquireBeforeTimeout() ? Permit.ACQUIRED : Permit.BORROWED;
            } catch (InterruptedException e) {
                throw interrupted(e);
            }
        }

        @Override
        public void afterRelease(Permit permit) {
            if (permit == Permit.BORROWED) {
                return;
            }
            super.afterRelease(permit);
        }
    }

    private static final class WaitOrFailStrategy extends TimedSemaphoreStrategy {
        private WaitOrFailStrategy(int maxActive, long timeoutMillis) {
            super(maxActive, timeoutMillis);
        }

        @Override
        public Permit beforeAcquire() {
            try {
                if (!tryAcquireBeforeTimeout()) {
                    throw exhausted("Timed out waiting for pool permit");
                }
                return Permit.ACQUIRED;
            } catch (InterruptedException e) {
                throw interrupted(e);
            }
        }
    }
}
