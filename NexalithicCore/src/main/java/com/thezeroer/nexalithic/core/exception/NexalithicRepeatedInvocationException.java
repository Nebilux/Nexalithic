package com.thezeroer.nexalithic.core.exception;

import java.util.Objects;

/**
 * Nexalithic重复调用异常。
 *
 * <p>当一个仅允许执行一次的方法被重复调用时抛出。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/23
 */
public final class NexalithicRepeatedInvocationException extends NexalithicException {

    /**
     * 被重复调用的方法名称。
     */
    private final String methodName;

    public NexalithicRepeatedInvocationException(String methodName) {
        this(null, methodName);
    }

    public NexalithicRepeatedInvocationException(Class<?> declaringType, String methodName) {
        super(createMessage(declaringType, methodName));
        this.methodName = Objects.requireNonNull(methodName, "methodName");
    }

    public String getMethodName() {
        return methodName;
    }

    private static String createMessage(Class<?> declaringType, String methodName) {
        Objects.requireNonNull(methodName, "methodName");
        if (declaringType == null) {
            return "Method may only be invoked once: " + methodName;
        }
        return "Method may only be invoked once: " + declaringType.getName() + "#" + methodName;
    }
}