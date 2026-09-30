package com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Handler 路径中的一个匹配层级。
 *
 * <p>该注解只能作为 {@link NexalithicHandlerController#levels()} 或
 * {@link NexalithicHandlerMethod#levels()} 的数组元素使用，用于描述路径中的单个层级。
 * 非空候选值表示该层级可以匹配其中任一值；候选值为空时表示通配符。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/24
 */
@Documented
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface HandlerPathLevel {

    /**
     * 该层级允许的候选值。
     * <p>若数组为空 {@code {}}，则视为<b>通配符（Wildcard）</b>，匹配该层级的所有值。</p>
     *
     * @return 当前层级允许匹配的候选值
     */
    short[] value() default {};
}
