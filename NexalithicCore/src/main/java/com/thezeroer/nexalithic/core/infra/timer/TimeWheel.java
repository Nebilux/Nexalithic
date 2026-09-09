package com.thezeroer.nexalithic.core.infra.timer;

import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;
import com.thezeroer.nexalithic.core.infra.recyclable.GenericWrapperPool;
import com.thezeroer.nexalithic.core.infra.recyclable.SelfStaticRecyclableWrapper;
import com.thezeroer.nexalithic.core.infra.recyclable.WrapperPool;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 基于单 Worker 线程推进的哈希时间轮。
 *
 * <p>一个实例只能启动一次。调用 {@link #stop()} 后不再接受新调度，也不能重新启动。</p>
 *
 * <p>时间轮不保证具有相同到期时间的目标按照提交顺序执行回调。等待队列允许多个生产者并发提交，
 * 调度节点转入槽位时采用头部合并，因此调用方不得依赖同一 tick 内的回调先后顺序；
 * 存在顺序依赖的业务应在时间轮之外进行串行化。</p>
 *
 * <p>多个生产者可以并发调用 {@link #schedule(Object, long, TimerCoordinator)}，但调用方必须负责
 * 协调提交线程与生命周期操作：调用 {@link #stop()} 前必须先停止并等待所有生产者退出，且从
 * {@code stop()} 开始执行后不得再提交目标。本类不会在 {@code schedule()} 与 {@code stop()} 之间建立
 * 提交—清理屏障；{@code acceptingSchedules} 仅用于尽早拒绝已经观察到停止状态的提交，不能替代调用方对生命周期的串行化。</p>
 *
 * @param <T> 被调度的目标类型
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/20
 */
public class TimeWheel<T> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, TimeWheel.class);
    /**
     * 时间轮配置项。
     */
    public static class Options extends OptionsDefinition {
        /** 单个 tick 的时长，单位为毫秒。
         *  <p>默认{@code 1_000L}，约{@code 1.074}秒 </p>
         */
        public final NexalithicOption<Long> TickMillis = TickMillis();
        /** 时间轮槽位数量。
         *  <p>默认{@code 64}</p>
         */
        public final NexalithicOption<Integer> SlotCount = SlotCount();
        /**
         * 等待队列转移配额的位移量。
         * 实际配额约为单个 tick 时长的 {@code 1 / (2^shift)}；
         * 例如 {@code 2} 代表 25%，{@code 3} 代表 12.5%。
         *  <p>默认{@code 2}</p>
         */
        public final NexalithicOption<Integer> TickQuotaShift = TickQuotaShift();
        /** 等待队列的初始分块大小。
         *  <p>默认{@code 1024}</p>
         */
        public final NexalithicOption<Integer> WaitQueue_ChunkSize = WaitQueue_ChunkSize();
        /** 调度节点池的建议容量。
         *  <p>默认{@code 256}</p>
         */
        public final NexalithicOption<Integer> WrapperPool_Capacity = WrapperPool_Capacity();
        protected Options(Class<?> holder) {
            super(holder);
        }
        protected NexalithicOption<Long> TickMillis() {
            return defineOption(1_000L, OptionValidator.positive());
        }
        protected NexalithicOption<Integer> SlotCount() {
            return defineOption(64, OptionValidator.positive());
        }
        protected NexalithicOption<Integer> TickQuotaShift() {
            return defineOption(2, OptionValidator.range(1, 63));
        }
        protected NexalithicOption<Integer> WaitQueue_ChunkSize() {
            return defineOption(1024, OptionValidator.positive());
        }
        protected NexalithicOption<Integer> WrapperPool_Capacity() {
            return defineOption(256, OptionValidator.positive());
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(TimeWheel.class);
    /** {@code log2(tickNanos)}，用于把纳秒差换算为 tick 序号。 */
    private final int tickShift;
    /** {@code slotCount - 1}，用于通过按位与定位槽位。 */
    private final int slotMask;
    /** {@code log2(slotCount)}，用于通过位移计算剩余完整轮数。 */
    private final int slotShift;
    /** 每个 tick 可用于转移等待队列节点的时间配额，单位为纳秒。 */
    private final long transferQuotaNanos;
    /** Worker 独占访问的槽位头节点数组。 */
    private final ScheduleWrapper<T>[] buckets;
    /** 调度节点分配与回收所使用的对象池。 */
    private final WrapperPool<ScheduleWrapper<T>> wrapperPool;
    /** 多生产者向单 Worker 提交调度节点的入口队列。 */
    private final MpscUnboundedArrayQueue<ScheduleWrapper<T>> waitQueue;
    /** 未单独指定协调器时使用的默认协调器，可以为空。 */
    private final TimerCoordinator<T> defaultCoordinator;
    /** 推进时间轮并独占管理槽位的单一线程。 */
    private final Worker worker;
    /** 生命周期入口开关；停止后永久关闭。 */
    private volatile boolean acceptingSchedules = true;

    /**
     * 创建时间轮。
     *
     * <p>{@code tickMillis} 与 {@code slotCount} 会向上规范化为 2 的幂。
     * 调用方应以规范化后的精度理解实际触发误差。</p>
     *
     * @param tickMillis 参见{@link Options#TickMillis}
     * @param slotCount 参见{@link Options#SlotCount}
     * @param tickQuotaShift 参见{@link Options#TickQuotaShift}
     * @param waitQueueChunkSize 参见{@link Options#WaitQueue_ChunkSize}
     * @param wrapperPool 调度节点池，不可为空
     * @param defaultCoordinator 默认协调器；允许为空，但此时每次调度都必须显式提供协调器
     * @param name Worker 名称后缀；为空时使用默认名称
     * @throws IllegalArgumentException 当 {@code tickQuotaShift} 不在有效范围内，或 {@code tickMillis}/{@code slotCount} 无法向上规范化为正数 2 次幂时抛出
     * @throws NullPointerException 当 {@code wrapperPool} 为空时抛出
     */
    @SuppressWarnings("unchecked")
    public TimeWheel(long tickMillis, int slotCount, int tickQuotaShift, int waitQueueChunkSize, WrapperPool<ScheduleWrapper<T>> wrapperPool, TimerCoordinator<T> defaultCoordinator, String name) {
        if (tickQuotaShift < 1 || tickQuotaShift > 63) {
            throw new IllegalArgumentException("tickQuotaShift must be between 1 and 63");
        }
        long normalizedTickNanos = normalize(TimeUnit.MILLISECONDS.toNanos(tickMillis));
        int normalizedSlotCount = normalize(slotCount);
        this.tickShift = Long.numberOfTrailingZeros(normalizedTickNanos);
        this.slotMask = normalizedSlotCount - 1;
        this.slotShift = Long.numberOfTrailingZeros(normalizedSlotCount);
        this.transferQuotaNanos = Math.max(1L, normalizedTickNanos >> tickQuotaShift);
        this.buckets = (ScheduleWrapper<T>[]) new ScheduleWrapper[normalizedSlotCount];
        this.wrapperPool = Objects.requireNonNull(wrapperPool, "wrapperPool");
        this.waitQueue = new MpscUnboundedArrayQueue<>(waitQueueChunkSize);
        this.defaultCoordinator = defaultCoordinator;
        this.worker = new Worker(name);
    }
    /**
     * 创建使用默认 Worker 名称的时间轮。
     *
     * @see #TimeWheel(long, int, int, int, WrapperPool, TimerCoordinator, String)
     */
    public TimeWheel(long tickMillis, int slotCount, int tickQuotaShift, int waitQueueChunkSize, WrapperPool<ScheduleWrapper<T>> wrapperPool, TimerCoordinator<T> defaultCoordinator) {
        this(tickMillis, slotCount, tickQuotaShift, waitQueueChunkSize, wrapperPool, defaultCoordinator, null);
    }
    /**
     *
     * 创建不绑定默认协调器的时间轮。
     *
     * <p>使用该构造方法后，只能调用显式接收 {@link TimerCoordinator} 的 {@code schedule} 重载；
     * 调用依赖默认协调器的重载会因为协调器为空而失败。</p>
     *
     * @see #TimeWheel(long, int, int, int, WrapperPool, TimerCoordinator, String)
     */
    public TimeWheel(long tickMillis, int slotCount, int tickQuotaShift, int waitQueueChunkSize, WrapperPool<ScheduleWrapper<T>> wrapperPool, String name) {
        this(tickMillis, slotCount, tickQuotaShift, waitQueueChunkSize, wrapperPool, null, name);
    }
    /**
     * 创建不绑定默认协调器并使用默认 Worker 名称的时间轮。
     *
     * <p>使用该构造方法后，只能调用显式接收 {@link TimerCoordinator} 的 {@code schedule} 重载；
     * 调用依赖默认协调器的重载会因为协调器为空而失败。</p>
     *
     * @see #TimeWheel(long, int, int, int, WrapperPool, TimerCoordinator, String)
     */
    public TimeWheel(long tickMillis, int slotCount, int tickQuotaShift, int waitQueueChunkSize, WrapperPool<ScheduleWrapper<T>> wrapperPool) {
        this(tickMillis, slotCount, tickQuotaShift, waitQueueChunkSize, wrapperPool, null, null);
    }

    /**
     * 启动 Worker 并开始推进时间轮。
     *
     * <p>底层 Worker 是一次性线程，因此本方法只能成功调用一次。时间轮停止后也不能重新启动。</p>
     *
     * @throws IllegalStateException 当时间轮已经停止，或底层线程已经启动过时抛出
     */
    public synchronized void start() {
        if (!acceptingSchedules) {
            throw new IllegalStateException("TimeWheel has been stopped");
        }
        worker.start();
    }
    /**
     * 停止时间轮并回收尚未完成的调度节点。
     *
     * <p>若 Worker 尚未启动，当前线程会直接清空等待队列和全部槽位；若 Worker 已经启动，
     * 本方法通过中断请求其退出，最终清理由 Worker 的 {@code finally} 块完成。本方法可以重复调用。</p>
     *
     * <p>调用方必须在调用本方法前停止并等待所有提交线程退出。本方法不得与任何 {@code schedule()}
     * 调用并发执行，否则提交可能发生在最终清理之后，导致节点留在无人消费的等待队列中。</p>
     */
    public synchronized void stop() {
        if (!acceptingSchedules) {
            return;
        }
        acceptingSchedules = false;
        if (worker.getState() == Thread.State.NEW) {
            clearScheduled();
        } else {
            worker.interrupt();
        }
    }

    /**
     * 使用指定标识和协调器提交一个调度目标。
     *
     * @param target 调度目标
     * @param stamp 由调用方定义的不透明注册标识
     * @param coordinator 当前注册使用的协调器
     * @throws NullPointerException 当coordinator为null时抛出
     * @throws IllegalStateException 当时间轮已经停止、节点池返回空值或等待队列拒绝提交时抛出
     * @apiNote 本方法允许由多个生产者并发调用，但不得与 {@link #stop()} 并发；生命周期协调由调用方负责
     */
    public void schedule(T target, long stamp, TimerCoordinator<T> coordinator) {
        Objects.requireNonNull(coordinator, "TimerCoordinator");
        if (!acceptingSchedules) {
            throw new IllegalStateException("TimeWheel has been stopped");
        }
        ScheduleWrapper<T> wrapper = wrapperPool.acquire();
        if (wrapper == null) {
            throw new IllegalStateException("ScheduleWrapper pool returned null");
        }
        waitQueue.offer(wrapper.wrap(target, stamp, coordinator));
    }
    /**
     * 使用指定标识和默认协调器提交目标。
     *
     * @param target 调度目标
     * @param stamp 由调用方定义的不透明注册标识
     * @see #schedule(T, long, TimerCoordinator)
     */
    public void schedule(T target, long stamp) {
        schedule(target, stamp, defaultCoordinator);
    }
    /**
     * 使用标识 {@code 0} 和指定协调器提交目标。
     *
     * @param target 调度目标
     * @param coordinator 当前注册使用的协调器
     * @see #schedule(T, long, TimerCoordinator)
     */
    public void schedule(T target, TimerCoordinator<T> coordinator) {
        schedule(target, 0, coordinator);
    }
    /**
     * 使用标识 {@code 0} 和默认协调器提交目标。
     *
     * @param target 调度目标
     * @see #schedule(T, long, TimerCoordinator)
     */
    public void schedule(T target) {
        schedule(target, 0, defaultCoordinator);
    }

    private static int normalize(int value) {
        if (value <= 1) {
            return 1;
        }
        int n = Integer.highestOneBit(value);
        if (n == value) {
            return n;
        }
        if (n > Integer.MAX_VALUE / 2) {
            throw new IllegalArgumentException("Value is too large to normalize to a positive power of two: " + value);
        }
        return n << 1;
    }
    private static long normalize(long value) {
        if (value <= 1) {
            return 1;
        }
        long n = Long.highestOneBit(value);
        if (n == value) {
            return n;
        }
        if (n > Long.MAX_VALUE / 2) {
            throw new IllegalArgumentException("Value is too large to normalize to a positive power of two: " + value);
        }
        return n << 1;
    }

    /**
     * 处理指定槽位在当前 tick 中的全部节点。
     */
    private void tick(long tickIndex) {
        int slot = (int) (tickIndex & slotMask);
        transferQueueToBuckets(tickIndex, slot);
        ScheduleWrapper<T> current = buckets[slot];
        buckets[slot] = null;
        ScheduleWrapper<T> unexpiredHead = null;
        ScheduleWrapper<T> unexpiredTail = null;
        while (current != null) {
            ScheduleWrapper<T> next = current.getNext();
            current.setNext(null);
            try {
                TimerCoordinator<T> coordinator = coordinatorOf(current);
                if (coordinator.isCancelled(current)) {
                    current.recycle();
                } else {
                    if (current.remainingRounds > 0) {
                        current.remainingRounds--;
                        if (unexpiredTail == null) {
                            unexpiredHead = current;
                        } else {
                            unexpiredTail.setNext(current);
                        }
                        unexpiredTail = current;
                    } else {
                        if (!acceptingSchedules || coordinator.onExpiryTrigger(current)) {
                            current.recycle();
                        } else {
                            waitQueue.offer(current);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("TimeWheel task processing failed. Context: {}", current, e);
                current.recycle();
            }
            current = next;
        }
        if (unexpiredHead != null) {
            mergeBack(slot, unexpiredHead, unexpiredTail);
        }
    }

    /**
     * 在当前 tick 的时间配额内，将等待队列中的新注册映射到对应槽位。
     *
     * <p>等待队列是多生产者与单 Worker 的线程边界。只有从该方法开始，节点才会进入 Worker 独占的
     * 槽位链表。每处理 64 个节点检查一次配额截止时间，以减少每个节点调用时钟方法的成本；
     * 因此配额是近似上限，而不是严格的实时保证。</p>
     */
    private void transferQueueToBuckets(long tickIndex, int slot) {
        long startNanos = System.nanoTime();
        int processed = 0;
        ScheduleWrapper<T> wrapper;
        while ((wrapper = waitQueue.poll()) != null) {
            try {
                TimerCoordinator<T> coordinator = coordinatorOf(wrapper);
                if (coordinator.isCancelled(wrapper)) {
                    wrapper.recycle();
                } else {
                    long deadlineTick = deadlineTick(coordinator.getExpiryTimeNanos(wrapper));
                    if (deadlineTick <= tickIndex) {
                        // 已到期节点放入当前槽位，并在本次 tick 随后的槽位遍历中触发。
                        wrapper.remainingRounds = 0;
                        mergeBack(slot, wrapper, wrapper);
                    } else {
                        long distance = deadlineTick - tickIndex;
                        // 高位表示抵达目标槽位前还需经过多少个完整轮次。
                        wrapper.remainingRounds = distance >> slotShift;
                        mergeBack((int) (deadlineTick & slotMask), wrapper, wrapper);
                    }
                }
            } catch (Exception e) {
                logger.error("TimeWheel task processing failed. Context: {}", wrapper, e);
                wrapper.recycle();
            }
            if ((++processed & 63) == 0 && System.nanoTime() - startNanos >= transferQuotaNanos) {
                break;
            }
        }
    }

    /**
     * 把基于 {@link System#nanoTime()} 的绝对纳秒到期时间转换为相对于 Worker 启动时刻的 tick 序号。
     *
     * <p>使用向上取整除法：只要到期时间不恰好落在 tick 边界，就安排到下一个 tick，
     * 避免因向下取整而提前触发。早于或等于启动时刻的时间统一映射为 tick 0。</p>
     *
     * @param expiryTime 基于 {@link System#nanoTime()} 的绝对到期时间，单位为纳秒
     * @return 对应的非负 tick 序号
     */
    private long deadlineTick(long expiryTime) {
        long elapsedNanos = expiryTime - worker.startTimeNanos;
        if (elapsedNanos <= 0) {
            return 0;
        }
        return ((elapsedNanos - 1) >> tickShift) + 1;
    }

    private TimerCoordinator<T> coordinatorOf(ScheduleWrapper<T> wrapper) {
        TimerCoordinator<T> coordinator = wrapper.coordinator;
        if (coordinator == null) {
            coordinator = defaultCoordinator;
        }
        if (coordinator == null) {
            throw new IllegalStateException("No TimerCoordinator bound to schedule");
        }
        return coordinator;
    }

    private void mergeBack(int slot, ScheduleWrapper<T> head, ScheduleWrapper<T> tail) {
        ScheduleWrapper<T> current = buckets[slot];
        tail.setNext(current);
        buckets[slot] = head;
    }

    private void clearScheduled() {
        ScheduleWrapper<T> wrapper;
        while ((wrapper = waitQueue.poll()) != null) {
            wrapper.recycle();
        }
        for (int slot = 0; slot < buckets.length; slot++) {
            ScheduleWrapper<T> current = buckets[slot];
            buckets[slot] = null;
            while (current != null) {
                ScheduleWrapper<T> next = current.getNext();
                current.recycle();
                current = next;
            }
        }
    }

    /**
     * 时间轮内部使用的池化调度节点。
     *
     * @param <T> 被调度的目标类型
     */
    public static class ScheduleWrapper<T> extends SelfStaticRecyclableWrapper<ScheduleWrapper<T>> implements TimerContext<T> {
        private ScheduleWrapper<T> next;
        private T target;
        private long stamp;
        private long remainingRounds;
        private TimerCoordinator<T> coordinator;

        public ScheduleWrapper(GenericWrapperPool<ScheduleWrapper<T>, ScheduleWrapper<T>> owner) {
            super(owner);
        }

        /** {@inheritDoc} */
        @Override
        public long targetStamp() {
            return stamp;
        }

        /** {@inheritDoc} */
        @Override
        public T target() {
            return target;
        }

        @Override
        protected void onReset() {
            next = null;
            target = null;
            stamp = 0;
            remainingRounds = 0;
            coordinator = null;
        }

        private ScheduleWrapper<T> wrap(T target, long stamp, TimerCoordinator<T> coordinator) {
            this.target = target;
            this.stamp = stamp;
            this.coordinator = coordinator;
            return this;
        }

        private void setNext(ScheduleWrapper<T> next) {
            this.next = next;
        }
        private ScheduleWrapper<T> getNext() {
            return next;
        }
    }

    private class Worker extends Thread {
        private long startTimeNanos;
        private long nextTick;

        public Worker(String name) {
            setDaemon(true);
            if (name == null) {
                setName("TimeWheel-Worker");
            } else {
                setName("TimeWheel-Worker: " + name);
            }
        }

        @Override
        public void run() {
            startTimeNanos = System.nanoTime();
            logger.debug("[{}] started", getName());
            try {
                while (!isInterrupted()) {
                    long now = waitNextTick();
                    if (now < 0) {
                        break;
                    }
                    long elapsedTick = now >> tickShift;
                    while (nextTick <= elapsedTick) {
                        tick(nextTick++);
                        if (isInterrupted()) {
                            break;
                        }
                    }
                }
            } finally {
                acceptingSchedules = false;
                clearScheduled();
                logger.debug("[{}] stopped", getName());
            }
        }

        /**
         * 等待下一个 tick 边界。
         *
         * @return 当前相对纳秒数；线程被中断时返回 {@code -1}
         */
        private long waitNextTick() {
            long deadline = nextTick << tickShift;
            while (true) {
                long now = System.nanoTime() - startTimeNanos;
                long sleep = deadline - now;
                if (sleep <= 0) {
                    return now;
                }
                if (sleep > 1_000_000L) {
                    LockSupport.parkNanos(sleep);
                } else {
                    Thread.onSpinWait();
                }
                if (Thread.interrupted()) {
                    return -1;
                }
            }
        }
    }
}
