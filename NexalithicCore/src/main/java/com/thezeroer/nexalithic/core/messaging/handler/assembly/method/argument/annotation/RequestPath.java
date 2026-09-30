package com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/06
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface RequestPath {
}
