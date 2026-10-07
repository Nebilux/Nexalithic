package com.nebilux.nexalithic.core.infra.concurrent.atomic;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 原子状态队列。
 *
 * <p>队列通过 {@link StateHandle} 共享状态。状态检查和元素入队构成一个受协调的操作：
 * 当状态转换方法返回时，所有基于旧状态通过准入检查的入队操作都已经结束。</p>
 *
 * <p>多个 {@code AtomicStateQueue} 可以共享同一个 {@code StateHandle}，
 * 从而建立跨队列的统一状态转换边界。</p>
 *
 * <p>该类只负责协调状态与入队操作，不会改变底层队列本身的并发能力。
 * 当多个线程并发访问队列时，调用方必须提供符合相应并发模型的
 * {@link Queue} 实现。</p>
 *
 * @param <E> 队列元素类型
 * @param <S> 状态类型
 *
 * @author Reonvia
 * @since 0.2.2
 */
public class AtomicStateQueue<E, S> {
    /**
     * 可在 {@link AtomicStateQueue} 之间共享的状态句柄。
     *
     * <p>句柄将状态值、状态代际和在途入队操作关联在一起。多个队列共享同一个
     * 句柄时，它们的入队操作会参与同一个状态转换屏障。状态转换与所有共享该
     * 句柄的队列入队操作之间具有以下保证：</p>
     *
     * <ul>
     *     <li>状态转换发布新状态后，新的入队操作按照新状态判断；</li>
     *     <li>已经按照旧状态获得准入的入队操作可以继续完成；</li>
     *     <li>状态转换返回前，会等待这些旧状态入队操作全部结束。</li>
     * </ul>
     *
     * <p>状态转换发布新快照后才等待旧代际收敛，因此其他线程可能在转换方法
     * 返回前观察到目标状态。转换方法返回则表示旧代际中已经通过快照确认的
     * 入队操作均已退出，不表示共享队列已经为空或其中的元素已经处理完毕。</p>
     *
     * <p>该句柄使用两个交替的计数槽位区分相邻状态代际。状态值的数量不受槽位
     * 数量限制；所有状态转换会被串行化，以保证计数槽位在复用前已经收敛。</p>
     *
     * <p>准入策略和底层队列的 {@code offer} 实现不得同步调用同一个句柄的状态
     * 转换方法，否则当前入队操作会等待自身退出。它们也应当快速完成，因为状态
     * 转换需要等待已经获得准入的入队操作结束。</p>
     *
     * @param <S> 状态类型
     */
    public static class StateHandle<S> {
        /** 自旋等待转换或代际收敛的最大连续次数。 */
        private static final int SPIN_LIMIT = 64;
        /** 超过自旋次数后，每次短暂挂起的纳秒数。 */
        private static final long PARK_NANOS = 1_000L;
        /**
         * 将状态和活动计数槽位放在同一个不可变快照中。
         *
         * <p>{@code slot} 只允许为 {@code 0} 或 {@code 1}，表示该状态代际使用
         * {@code activeOffers} 中的哪一个计数槽位。它不是状态编号，也不限制
         * 状态类型可以包含的状态数量。</p>
         *
         * <p>每次状态转换都会创建新快照，包括目标状态对象与当前状态对象相同的
         * 情况。入队操作通过快照引用判断读取后是否发生过转换，自身不会创建
         * 快照对象。</p>
         */
        private record StateSnapshot<S>(S value, int slot) {}
        /**
         * 当前已经发布的状态快照。
         *
         * <p>状态值和计数槽位通过同一个原子引用发布，避免入队操作观察到来自
         * 不同状态代际的值与槽位。</p>
         */
        private final AtomicReference<StateSnapshot<S>> state;
        /**
         * 两个交替状态代际中的在途入队操作数量。
         *
         * <p>计数覆盖快照确认、准入策略判断和底层队列入队，不表示队列大小或
         * 成功入队的元素数量。共享该句柄的所有队列共同使用这两个计数槽位。</p>
         */
        private final AtomicIntegerArray activeOffers = new AtomicIntegerArray(2);
        /**
         * 串行化状态转换。
         *
         * <p>入队操作不竞争该标记；只有修改状态或建立代际屏障的方法之间会发生
         * 竞争。串行化转换可以保证下一个转换复用计数槽位前，上一个旧代际已经
         * 收敛。</p>
         */
        private final AtomicBoolean transitioning = new AtomicBoolean();

        /**
         * 创建具有指定初始状态的状态句柄。
         *
         * @param initialState 初始状态，可以为 {@code null}
         */
        public StateHandle(S initialState) {
            state = new AtomicReference<>(new StateSnapshot<>(initialState, 0));
        }
        /**
         * 创建初始状态为 {@code null} 的状态句柄。
         */
        public StateHandle() {
            this(null);
        }

        /**
         * 返回当前已经发布的状态。
         *
         * @return 当前状态
         */
        public S get() {
            return state.get().value();
        }

        /**
         * 设置新状态。
         *
         * <p>新状态会先被发布，然后等待所有基于旧状态获得准入的
         * 入队操作结束。因此其他线程可能在本方法返回前观察到新状态，
         * 但本方法返回后不会再有旧状态准入的元素进入共享队列。</p>
         *
         * <p>即使新状态对象与当前状态对象相同，本方法仍然会建立一次
         * 新的状态边界。</p>
         *
         * @param targeted 目标状态，可以为 {@code null}
         */
        public void set(S targeted) {
            getAndSet(targeted);
        }

        /**
         * 设置新状态并返回转换前的状态。
         *
         * <p>目标状态会通过一个使用另一计数槽位的新快照发布。本方法随后等待
         * 所有基于旧快照获得准入的入队操作结束。即使目标状态对象与当前状态
         * 对象相同，也会建立新的状态代际边界。</p>
         *
         * @param targeted 目标状态，可以为 {@code null}
         * @return 转换前的状态
         */
        public S getAndSet(S targeted) {
            acquireTransition();
            try {
                StateSnapshot<S> previous = state.get();
                StateSnapshot<S> next = new StateSnapshot<>(targeted, 1 - previous.slot());
                state.set(next);
                awaitQuiescence(previous.slot());
                return previous.value();
            } finally {
                releaseTransition();
            }
        }

        /**
         * 当当前状态与预期状态为同一对象时切换状态。
         *
         * <p>状态比较遵循 {@link AtomicReference#compareAndSet(Object, Object)}
         * 的引用相等语义，即使用 {@code ==} 而不是
         * {@link Objects#equals(Object, Object)}。</p>
         *
         * <p>转换成功后，本方法会等待所有基于旧状态获得准入的
         * 入队操作结束。</p>
         *
         * @param expected 预期状态
         * @param targeted 目标状态
         * @return 状态匹配并成功转换时返回 {@code true}，
         *         否则返回 {@code false}
         */
        public boolean compareAndSet(S expected, S targeted) {
            return compareAndExchange(expected, targeted) == expected;
        }

        /**
         * 当当前状态与预期状态为同一对象时切换状态，并返回实际观察到的状态。
         *
         * <p>状态比较采用引用相等语义。观察到的状态不是 {@code expected} 时，
         * 本方法直接返回该状态，不发布新快照；匹配时发布目标状态，等待旧代际
         * 入队操作结束，然后返回转换前的状态。</p>
         *
         * @param expected 预期状态
         * @param targeted 目标状态，可以为 {@code null}
         * @return 调用期间实际观察到的状态；返回值与 {@code expected} 为同一
         *         对象表示转换成功
         */
        public S compareAndExchange(S expected, S targeted) {
            acquireTransition();
            try {
                StateSnapshot<S> previous = state.get();
                if (previous.value() != expected) {
                    return previous.value();
                }
                StateSnapshot<S> next = new StateSnapshot<>(targeted, 1 - previous.slot());
                /*
                 * 所有状态转换都由 transitioning 串行化，因此这里理论上
                 * 不会失败。保留 CAS 可以防止未来新增绕过转换协议的写入。
                 */
                if (!state.compareAndSet(previous, next)) {
                    throw new IllegalStateException(
                            "State changed outside the transition protocol"
                    );
                }
                awaitQuiescence(previous.slot());
                return previous.value();
            } finally {
                releaseTransition();
            }
        }

        /**
         * 当当前状态满足指定条件时切换状态。
         *
         * <p>条件在获得状态转换资格后执行一次。条件不满足时不会发布新快照，
         * 也不会建立新的代际边界；条件满足时，本方法会发布目标状态并等待旧
         * 代际入队操作结束。</p>
         *
         * <p>条件实现不得调用同一个句柄的状态转换方法，也不应执行长时间阻塞
         * 操作。</p>
         *
         * @param condition 根据当前状态判断是否允许转换的条件
         * @param targeted 目标状态，可以为 {@code null}
         * @return 条件满足并完成状态转换时返回 {@code true}，否则返回
         *         {@code false}
         * @throws NullPointerException 当 {@code condition} 为 {@code null} 时抛出
         */
        public boolean transitionIf(Predicate<? super S> condition, S targeted) {
            Objects.requireNonNull(condition, "condition");
            acquireTransition();
            try {
                StateSnapshot<S> previous = state.get();
                if (!condition.test(previous.value())) {
                    return false;
                }
                StateSnapshot<S> next = new StateSnapshot<>(targeted, 1 - previous.slot());
                if (!state.compareAndSet(previous, next)) {
                    throw new IllegalStateException(
                            "State changed outside the transition protocol"
                    );
                }
                awaitQuiescence(previous.slot());
                return true;
            } finally {
                releaseTransition();
            }
        }

        /**
         * 在不改变状态值的情况下建立新的状态代际边界。
         *
         * <p>本方法使用当前状态值发布一个采用另一计数槽位的新快照，并等待调用
         * 前已经基于旧快照获得准入的入队操作结束。调用期间产生的新入队操作可
         * 使用新快照继续执行，因此本方法不保证队列为空，也不保证返回时不存在
         * 新状态代际中的在途入队操作。</p>
         */
        public void barrier() {
            acquireTransition();
            try {
                StateSnapshot<S> previous = state.get();
                StateSnapshot<S> next = new StateSnapshot<>(previous.value(), 1 - previous.slot());
                state.set(next);
                awaitQuiescence(previous.slot());
            } finally {
                releaseTransition();
            }
        }

        /**
         * 按当前状态和准入策略向指定队列提交元素。
         *
         * <p>方法先登记为当前状态代际的参与者，再重新确认状态快照。快照已经
         * 变化时会撤销登记并使用新快照重试；快照未变化时，参与计数会一直覆盖
         * 准入策略判断和底层队列入队。</p>
         *
         * @param delegateQueue 接收元素的底层队列
         * @param element 待提交的元素
         * @param offerPolicy 根据当前状态和元素判断是否允许提交的策略
         * @param <E> 队列元素类型
         * @return 策略允许且底层队列接受元素时返回 {@code true}，否则返回
         *         {@code false}
         */
        private <E> boolean offer(Queue<E> delegateQueue, E element, BiPredicate<? super S, ? super E> offerPolicy) {
            while (true) {
                StateSnapshot<S> snapshot = state.get();
                int slot = snapshot.slot();
                /*
                 * 先登记为当前状态代际的参与者，再确认状态快照没有变化。
                 */
                activeOffers.incrementAndGet(slot);
                if (state.get() != snapshot) {
                    activeOffers.decrementAndGet(slot);
                    Thread.onSpinWait();
                    continue;
                }
                try {
                    /*
                     * 从重新确认快照开始，到 activeOffers 减少为止，
                     * 状态转换即使已经发布新状态，也必须等待本次操作结束。
                     */
                    if (!offerPolicy.test(snapshot.value(), element)) {
                        return false;
                    }
                    return delegateQueue.offer(element);
                } finally {
                    activeOffers.decrementAndGet(slot);
                }
            }
        }

        /**
         * 获取状态转换资格。
         *
         * <p>入队热路径不调用该方法，只有状态转换之间需要串行化。等待过程先
         * 自旋，超过阈值后短暂挂起；中断不会取消状态转换竞争，但会在获得转换
         * 资格后恢复中断标记。</p>
         */
        private void acquireTransition() {
            boolean interrupted = false;
            int spins = 0;
            while (!transitioning.compareAndSet(false, true)) {
                if (spins++ < SPIN_LIMIT) {
                    Thread.onSpinWait();
                    continue;
                }
                /*
                 * parkNanos 不用于互斥，只用于降低长时间竞争时的空转开销。
                 * 状态转换不能在已经发布部分状态后因中断而取消，因此记录
                 * 中断并在获得转换资格后恢复。
                 */
                if (Thread.interrupted()) {
                    interrupted = true;
                }
                LockSupport.parkNanos(PARK_NANOS);
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * 释放状态转换资格。
         *
         * <p>必须在状态转换方法的 {@code finally} 块中调用，确保转换失败或等待
         * 期间出现异常时，后续状态转换仍能继续。</p>
         */
        private void releaseTransition() {
            transitioning.set(false);
        }

        /**
         * 等待指定状态代际中已经获得准入的操作全部结束。
         *
         * <p>这里只等待指定旧槽位。状态转换发布新快照后产生的入队操作会登记
         * 到另一个槽位，不会阻止旧代际收敛。等待不可中断，但会在返回前恢复
         * 等待期间观察到的中断标记。</p>
         *
         * @param slot 需要等待的旧状态代际计数槽位，只能为 {@code 0} 或
         *             {@code 1}
         */
        private void awaitQuiescence(int slot) {
            boolean interrupted = false;
            int spins = 0;
            while (activeOffers.get(slot) != 0) {
                if (spins++ < SPIN_LIMIT) {
                    Thread.onSpinWait();
                    continue;
                }
                /*
                 * 状态已经发布，转换不能安全地因中断而回滚，因此采用
                 * 不可中断等待，并在完成后恢复中断标记。
                 */
                if (Thread.interrupted()) {
                    interrupted = true;
                }
                LockSupport.parkNanos(PARK_NANOS);
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private final Queue<E> delegateQueue;
    private final StateHandle<S> stateHandle;
    private final BiPredicate<? super S, ? super E> offerPolicy;

    public AtomicStateQueue(Queue<E> delegateQueue, StateHandle<S> stateHandle, BiPredicate<? super S, ? super E> offerPolicy) {
        this.delegateQueue = Objects.requireNonNull(delegateQueue, "delegateQueue");
        this.stateHandle = Objects.requireNonNull(stateHandle, "stateHandle");
        this.offerPolicy = Objects.requireNonNull(offerPolicy, "offerPolicy");
    }

    public AtomicStateQueue(Queue<E> delegateQueue, StateHandle<S> stateHandle) {
        this(delegateQueue, stateHandle, (s, e) -> true);
    }

    public AtomicStateQueue(Queue<E> delegateQueue, BiPredicate<? super S, ? super E> offerPolicy) {
        this(delegateQueue, new StateHandle<>(), offerPolicy);
    }

    public AtomicStateQueue(Queue<E> delegateQueue) {
        this(delegateQueue, new StateHandle<>(), (s, e) -> true);
    }

    /**
     * 根据当前状态和准入策略尝试加入元素。
     *
     * @param element 待加入的元素
     * @return 策略允许且底层队列接受元素时返回 {@code true}
     */
    public boolean offer(E element) {
        return stateHandle.offer(delegateQueue, element, offerPolicy);
    }

    /**
     * 返回队首元素但不移除。
     *
     * @return 队首元素；队列为空时返回 {@code null}
     */
    public E peek() {
        return delegateQueue.peek();
    }

    /**
     * 返回并移除队首元素。
     *
     * @return 队首元素；队列为空时返回 {@code null}
     */
    public E poll() {
        return delegateQueue.poll();
    }

    /**
     * 从队首移除元素并交给指定操作处理，直到队列被观察为空。
     *
     * <p>本方法只负责消费底层队列中的元素，不会读取或改变共享状态，也不会
     * 阻止并发提交。若其他线程持续提交元素，本方法可能长时间无法返回。需要
     * 最终排空队列时，调用方应先将共享状态转换为拒绝新提交的状态。</p>
     *
     * <p>元素会在调用 {@code action} 前从队列中移除。若操作抛出异常，该异常
     * 会直接传播，当前元素不会重新加入队列，剩余元素仍保留在队列中。</p>
     *
     * @param action 元素处理操作
     * @return 本次处理的元素数量
     * @throws NullPointerException 当 {@code action} 为 {@code null} 时抛出
     */
    public int drain(Consumer<? super E> action) {
        Objects.requireNonNull(action, "action");
        int drained = 0;
        E element;
        while ((element = delegateQueue.poll()) != null) {
            action.accept(element);
            if (drained != Integer.MAX_VALUE) {
                drained++;
            }
        }
        return drained;
    }

    /**
     * 从队首移除并处理至多 {@code limit} 个元素。
     *
     * <p>本方法不参与状态转换协议，也不阻止并发提交。它适用于事件循环中的
     * 有界批量处理，可避免单个队列长期占用消费线程。</p>
     *
     * <p>元素会在调用 {@code action} 前从队列中移除。若操作抛出异常，该异常
     * 会直接传播，当前元素不会重新加入队列，剩余元素仍保留在队列中。</p>
     *
     * @param action 元素处理操作
     * @param limit 本次最多处理的元素数量，必须大于或等于 {@code 0}
     * @return 本次实际处理的元素数量
     * @throws NullPointerException 当 {@code action} 为 {@code null} 时抛出
     * @throws IllegalArgumentException 当 {@code limit} 小于 {@code 0} 时抛出
     */
    public int drain(Consumer<? super E> action, int limit) {
        Objects.requireNonNull(action, "action");
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }
        int drained = 0;
        while (drained < limit) {
            E element = delegateQueue.poll();
            if (element == null) {
                break;
            }
            action.accept(element);
            drained++;
        }
        return drained;
    }

    /**
     * 判断底层队列是否为空。
     *
     * @return 队列为空时返回 {@code true}
     */
    public boolean isEmpty() {
        return delegateQueue.isEmpty();
    }

    /**
     * 返回该队列使用的共享状态句柄。
     *
     * @return 状态句柄
     */
    public StateHandle<S> state() {
        return stateHandle;
    }
}
