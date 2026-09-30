package com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明一个 Nexalithic Handler 方法。
 *
 * <p>该注解标记在 Controller 的实例方法上。装配阶段会将该方法转换为
 * {@code NexalithicHandler}，并把注解中的路径追加到 Controller 公共路径之后。</p>
 *
 * <p>被标记的方法必须满足装配器要求的 Handler 方法签名约束。</p>
 * @see NexalithicHandlerController
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/24
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NexalithicHandlerMethod {

    /**
     * 快捷方式：定义一条确定的线性多级路径。
     *
     * @return 线性路径层级
     * @see NexalithicHandlerController#value()
     */
    short[] value() default {};

    /**
     * 完整方式：定义包含候选值或通配符的多级路径。
     *
     * @return 完整路径层级声明
     * @see NexalithicHandlerController#levels()
     */
    HandlerPathLevel[] levels() default {};
}
