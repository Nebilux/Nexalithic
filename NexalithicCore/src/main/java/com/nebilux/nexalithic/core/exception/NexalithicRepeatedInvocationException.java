/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.exception;

import java.util.Objects;

/**
 * Nexalithic重复调用异常。
 *
 * <p>当一个仅允许执行一次的方法被重复调用时抛出。</p>
 *
 * @author Reonvia
 * @since 0.2.0
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