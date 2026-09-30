package com.thezeroer.nexalithic.core.builder;

import java.util.Objects;

/**
 * Nexalithic 通用配置器。
 *
 * <p>该接口与 {@link java.util.function.BiConsumer} 类似，用于把针对某个目标对象
 * 及其辅助对象的配置逻辑封装为可复用对象。第一个参数通常是 Builder 或其他可配置对象，
 * 第二个参数通常是上层构建过程提供的辅助信息或便捷能力。</p>
 *
 * <p>框架中的领域配置器可以继承该接口，并通过泛型固定具体的配置目标和辅助对象类型。</p>
 *
 * @param <T> 被配置的目标类型
 * @param <H> 配置辅助对象类型
 *
 * @author tbrtz647@outlook.com
 * @since 2026/08/02
 * @version 1.0.0
 */
@FunctionalInterface
public interface NexalithicConfigurer<T, H> {
    /**
     * 配置目标对象。
     *
     * @param target 被配置的目标对象
     * @param helper 配置辅助对象
     */
    void configure(T target, H helper);

    /**
     * 将当前配置器与另一个配置器按顺序组合。
     *
     * @param after 后续执行的配置器
     * @return 组合后的配置器
     */
    default NexalithicConfigurer<T, H> andThen(NexalithicConfigurer<? super T, ? super H> after) {
        Objects.requireNonNull(after, "after");
        return (target, helper) -> {
            configure(target, helper);
            after.configure(target, helper);
        };
    }
}
