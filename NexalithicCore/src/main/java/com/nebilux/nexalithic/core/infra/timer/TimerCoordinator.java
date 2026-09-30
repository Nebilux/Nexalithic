package com.nebilux.nexalithic.core.infra.timer;

/**
 * 定时器调度策略与到期回调的协调器。
 *
 * <p>{@link TimeWheel} 只负责按照时间推进调度，不理解目标对象 {@code T} 的生命周期。
 * 目标何时到期、当前注册是否已经取消，以及到期后是否结束调度，均由本接口决定。
 * 因而同一个时间轮既可以通过默认协调器服务同类目标，也可以在每次调度时绑定不同的协调器。</p>
 *
 * <p>本接口的三个方法都由时间轮的单一 Worker 线程同步调用。实现应尽量保持短小且非阻塞；
 * 如果到期动作需要执行耗时业务、获取竞争激烈的锁或切换到目标所属线程，建议在
 * {@link #onExpiryTrigger(TimerContext)} 中仅投递事件，由业务执行器等完成真正处理。</p>
 *
 * <p>传入的 {@link TimerContext} 只能在当前方法调用期间使用，
 * 不得保存到字段、集合或异步任务中。</p>
 *
 * @param <T> 被调度的目标类型
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/20
 */
public interface TimerCoordinator<T> {

    /**
     * 获取当前注册的绝对到期时间。
     *
     * <p>返回值必须使用与 {@link System#nanoTime()} 相同的单调时间基准，单位为纳秒。
     * 时间轮会将该时间点向上取整到最近的 tick，保证正常情况下不会早于指定时间触发。</p>
     *
     * <p>该方法会在调度节点从等待队列转入时间轮槽位时调用。若
     * {@link #onExpiryTrigger(TimerContext)} 返回 {@code false}，同一节点会重新进入等待队列，
     * 下一次转移时会再次读取到期时间，因此可以通过更新目标状态实现续期或刷新超时。</p>
     *
     * @param context 当前调度的临时只读上下文
     * @return 当前注册基于 {@link System#nanoTime()} 的绝对到期时间，单位为纳秒
     */
    long getExpiryTimeNanos(TimerContext<T> context);

    /**
     * 判断当前注册是否已经取消或失效。
     *
     * <p>返回 {@code true} 后，时间轮会直接移除并回收对应调度节点，不再调用
     * {@link #onExpiryTrigger(TimerContext)}。实现通常应同时校验目标状态与
     * {@link TimerContext#targetStamp()}，以识别刷新、复用或新一代生命周期留下的旧注册。</p>
     *
     * @param context 当前调度的临时只读上下文
     * @return {@code true} 表示丢弃当前注册；{@code false} 表示继续调度
     */
    boolean isCancelled(TimerContext<T> context);

    /**
     * 处理到期触发。
     *
     * <p>返回 {@code true} 表示当前注册已经处理完毕，时间轮会回收调度节点；
     * 返回 {@code false} 表示本次尚未结束，时间轮会把同一节点重新放入等待队列，
     * 并在后续 tick 重新读取到期时间。该返回值适合表达续期、条件尚未满足或需要再次检查的场景。</p>
     *
     * <p>若实现抛出异常，时间轮会记录错误并回收当前节点，不会让异常终止 Worker 线程。</p>
     *
     * @param context 已到期调度的临时只读上下文
     * @return {@code true} 表示结束并回收；{@code false} 表示重新调度
     */
    boolean onExpiryTrigger(TimerContext<T> context);
}
