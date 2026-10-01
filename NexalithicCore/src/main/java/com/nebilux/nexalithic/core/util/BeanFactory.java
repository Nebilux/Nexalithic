package com.nebilux.nexalithic.core.util;

/**
 * Nexalithic Bean 工厂
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface BeanFactory {
    <T> T getBean(Class<T> clazz);
    <T> T getBean(Class<T> clazz, Object... args);
}