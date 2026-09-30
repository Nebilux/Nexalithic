package com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 声明一个 Nexalithic Handler Controller。
 *
 * <p>该注解标记在 Controller 类上。装配器只会扫描被该注解标记的实例，
 * 并把注解中的路径作为该 Controller 下所有 Handler 方法的公共路径前缀。</p>
 *
 * <p>路径声明方式：</p>
 * <ul>
 *     <li>{@link #value()}用于快捷定义一条确定的线性路径；</li>
 *     <li>{@link #levels()}用于定义包含候选值或通配符的复杂路径；</li>
 *     <li>两者不能同时设置。</li>
 * </ul>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/24
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface NexalithicHandlerController {

    /**
     * 快捷方式：定义一条确定的线性多级路径。
     *
     * <p>数组中的每个元素对应一个路径层级，语义类似URL中的路径段。</p>
     *
     * <p>例如：</p>
     * <pre>{@code @NexalithicHandlerController({0x01, 0x02, 0x03})}</pre>
     *
     * <p>表示路径：</p>
     * <pre>{@code /0x01/0x02/0x03}</pre>
     *
     * <p>仅在 {@link #levels()} 为空时使用。</p>
     *
     * @return 线性路径层级
     */
    short[] value() default {};

    /**
     * 完整方式：定义包含候选值或通配符的多级路径。
     *
     * <p>每个 {@link HandlerPathLevel} 对应一个路径层级；
     * 一个层级中可以声明多个候选值。空候选值表示通配符。</p>
     *
     * @return 完整路径层级声明
     */
    HandlerPathLevel[] levels() default {};
}
