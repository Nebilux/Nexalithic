package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记 Handler 方法参数来自当前请求包中的 Payload。
 *
 * <p>注解值表示 Payload 在当前请求包中的下标。
 * 例如 {@code @RequestPayload(0)} 表示选择第一个 Payload。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/02
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface RequestPayload {
    /**
     * 请求包中的 Payload 下标。
     *
     * @return 从 0 开始的 Payload 下标
     */
    int value() default 0;
}
