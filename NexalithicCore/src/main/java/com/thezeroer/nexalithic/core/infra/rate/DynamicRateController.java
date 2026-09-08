package com.thezeroer.nexalithic.core.infra.rate;

import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;
import com.thezeroer.nexalithic.core.builder.option.OptionValidator;
import com.thezeroer.nexalithic.core.builder.option.OptionsDefinition;

import java.util.concurrent.TimeUnit;

/**
 * 会话动态限速控制器。基于 EWMA 流量预测的非对称负反馈控制系统
 * <p>根据一个采样周期内的吞吐，判断是否需要下发新的速率值（B/s）。
 * <ul>
 * <li>返回 > 0：发布该速率</li>
 * <li>返回 -1：本周期不发布</li>
 * </ul>
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/21
 * @version 1.0.0
 */
public class DynamicRateController {
    /**
     * 动态限速状态
     */
    public static final class RateState {
        double ewmaBps = -1;
        long lastPublishedRate = -1;
        long lastPublishedAtNanos = -1;
        int upStableTicks = 0;

        public void reset() {
            ewmaBps = -1;
            lastPublishedRate = -1;
            lastPublishedAtNanos = -1;
            upStableTicks = 0;
        }
    }

    /**
     * 控制器参数项定义。
     */
    public static class Options extends OptionsDefinition {
        /** 是否启用动态限速。 */
        public final NexalithicOption<Boolean> Enable = NexalithicOption.create(
                true, OptionValidator.nonNull()
        );
        /** 控制周期（毫秒）。 */
        public final NexalithicOption<Long> TickMillis = NexalithicOption.create(
                500L, OptionValidator.positive()
        );
        /** 最低下发速率（B/s）。 */
        public final NexalithicOption<Long> MinBps = NexalithicOption.create(
                1024L * 1024, OptionValidator.positive()
        );
        /** 最高下发速率（B/s）。 */
        public final NexalithicOption<Long> MaxBps = NexalithicOption.create(
                1024L * 1024 * 64, OptionValidator.positive()
        );
        /**
         * 初始下发速率（B/s）。
         * 首次发布直接使用该值，不从 MinBps 开始慢慢抬升。
         */
        public final NexalithicOption<Long> InitialBps = NexalithicOption.create(
                1024L * 1024 * 16, OptionValidator.positive()
        );
        /** EWMA 平滑系数：越大越灵敏，越小越平稳。 */
        public final NexalithicOption<Double> EwmaAlpha = NexalithicOption.create(
                0.3D, OptionValidator.unitInterval()
        );
        /** 冗余系数：目标速率 = 平滑吞吐 * Headroom。 */
        public final NexalithicOption<Double> Headroom = NexalithicOption.create(
                1.3D, OptionValidator.positive()
        );
        /** 相对变化阈值：变化小于该比例不发布。 */
        public final NexalithicOption<Double> ChangeThreshold = NexalithicOption.create(
                0.1D, OptionValidator.unitInterval()
        );
        /** 最小发布间隔（毫秒），限制控制面信令频率。 */
        public final NexalithicOption<Long> MinPublishIntervalMillis = NexalithicOption.create(
                500L, OptionValidator.positive()
        );
        /** 升速稳定周期数（慢升），降速始终立即生效（快降）。 */
        public final NexalithicOption<Integer> IncreaseStableTicks = NexalithicOption.create(
                3, OptionValidator.positive()
        );

        public Options(Class<?> holder) {
            super(holder);
        }
    }

    private final long minRateBps;
    private final long maxRateBps;
    private final long initialRateBps;
    private final double ewmaAlpha;
    private final double headroom;
    private final double changeThreshold;
    private final long minPublishIntervalNanos;
    private final int increaseStableTicks;

    public DynamicRateController(long minRateBps, long maxRateBps, long initialRateBps, double ewmaAlpha, double headroom,
                                 double changeThreshold, long minPublishIntervalMillis, int increaseStableTicks) {
        this.minRateBps = minRateBps;
        this.maxRateBps = maxRateBps;
        this.initialRateBps = initialRateBps;
        this.ewmaAlpha = ewmaAlpha;
        this.headroom = headroom;
        this.changeThreshold = changeThreshold;
        this.minPublishIntervalNanos = TimeUnit.MILLISECONDS.toNanos(minPublishIntervalMillis);
        this.increaseStableTicks = increaseStableTicks;
    }

    public long evaluateAndGetRate(long bytes, long intervalNanos, long nowNanos, RateState state) {
        if (intervalNanos <= 0) {
            return -1;
        }
        double instantBps = bytes <= 0 ? 0D : (bytes * 1_000_000_000D / intervalNanos);
        if (state.ewmaBps < 0) {
            state.ewmaBps = instantBps;
        } else {
            state.ewmaBps = state.ewmaBps * (1D - ewmaAlpha) + instantBps * ewmaAlpha;
        }
        if (state.lastPublishedRate < 0) {
            state.lastPublishedRate = clamp(initialRateBps, minRateBps, maxRateBps);
            state.lastPublishedAtNanos = nowNanos;
            state.upStableTicks = 0;
            return state.lastPublishedRate;
        }
        if (nowNanos - state.lastPublishedAtNanos < minPublishIntervalNanos) {
            return -1;
        }
        long target = clamp((long) (state.ewmaBps * headroom), minRateBps, maxRateBps);
        if (relativeDiff(target, state.lastPublishedRate) < changeThreshold) {
            state.upStableTicks = 0;
            return -1;
        }
        if (target < state.lastPublishedRate) {
            state.lastPublishedRate = target;
            state.lastPublishedAtNanos = nowNanos;
            state.upStableTicks = 0;
            return target;
        }
        if (++state.upStableTicks >= increaseStableTicks) {
            state.lastPublishedRate = target;
            state.lastPublishedAtNanos = nowNanos;
            state.upStableTicks = 0;
            return target;
        }
        return -1;
    }

    private static double relativeDiff(long a, long b) {
        long base = Math.max(1L, b);
        return Math.abs(a - b) / (double) base;
    }

    private static long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(max, value));
    }
}
