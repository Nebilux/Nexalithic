package com.thezeroer.nexalithic.core.builder.option;

/**
 * Option 验证器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/02
 * @version 1.0.0
 */
@FunctionalInterface
public interface OptionValidator<T> {
    /**
     * 验证参数是否合法
     * @param value 待验证的值
     * @throws IllegalArgumentException 如果验证失败，抛出带详细原因的异常
     */
    void validate(T value) throws IllegalArgumentException;

    /**
     * nonNull
     */
    static <T> OptionValidator<T> nonNull() {
        return value -> {
            if (value == null)
                throw new IllegalArgumentException("Value must not be null, but got: " + null);
        };
    }

    /**
     * 验证必须是正数 (> 0)
     */
    static <T extends Number> OptionValidator<T> positive() {
        return value -> {
            if (value == null || value.doubleValue() <= 0) {
                throw new IllegalArgumentException("Value must be positive, but got: " + value);
            }
        };
    }

    /**
     * 验证必须是非负数 (>= 0)
     */
    static <T extends Number> OptionValidator<T> nonNegative() {
        return value -> {
            if (value == null || value.doubleValue() < 0) {
                throw new IllegalArgumentException("Value must be non-negative, but got: " + value);
            }
        };
    }

    /**
     * 验证数值在闭区间 [min, max] 内
     */
    static <T extends Number> OptionValidator<T> closedInterval(double min, double max) {
        return value -> {
            if (value == null || value.doubleValue() < min || value.doubleValue() > max) {
                throw new IllegalArgumentException(
                        String.format("Value must be in range [%s, %s], but got: %s", min, max, value)
                );
            }
        };
    }

    /**
     * 验证数值在开区间 (min, max) 内
     */
    static <T extends Number> OptionValidator<T> openInterval(double min, double max) {
        return value -> {
            if (value == null || value.doubleValue() <= min || value.doubleValue() >= max) {
                throw new IllegalArgumentException(
                        String.format("Value must be in range (%s, %s), but got: %s", min, max, value)
                );
            }
        };
    }

    /**
     * 专门针对百分比或概率的验证 [0, 1]
     */
    static <T extends Number> OptionValidator<T> unitInterval() {
        return closedInterval(0.0, 1.0);
    }

    /**
     * 验证必须是 2 的幂 (常用于 Buffer 大小验证)
     */
    static OptionValidator<Integer> powerOfTwo() {
        return value -> {
            if (value == null || value <= 0 || (value & (value - 1)) != 0) {
                throw new IllegalArgumentException("Value must be a power of 2, but got: " + value);
            }
        };
    }

    /**
     * 最小值验证：必须 >= min
     */
    static <T extends Comparable<T>> OptionValidator<T> min(T min) {
        return value -> {
            if (value == null || value.compareTo(min) < 0) {
                throw new IllegalArgumentException(
                        String.format("Value must be at least %s, but got: %s", min, value)
                );
            }
        };
    }

    /**
     * 最大值验证：必须 <= max
     */
    static <T extends Comparable<T>> OptionValidator<T> max(T max) {
        return value -> {
            if (value == null || value.compareTo(max) > 0) {
                throw new IllegalArgumentException(
                        String.format("Value must be at most %s, but got: %s", max, value)
                );
            }
        };
    }

    /**
     * 区间验证：必须在 [min, max] 之间
     */
    static <T extends Comparable<T>> OptionValidator<T> closedInterval(T min, T max) {
        return value -> {
            if (value == null || value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                throw new IllegalArgumentException(
                        String.format("Value must be in range [%s, %s], but got: %s", min, max, value)
                );
            }
        };
    }
}