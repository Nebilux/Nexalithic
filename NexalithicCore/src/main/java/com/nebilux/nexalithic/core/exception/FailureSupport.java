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

import java.util.concurrent.CompletionException;

/**
 * 处理任意 {@link Throwable} 的内部工具，不限于 {@link NexalithicFailure}。
 * 两种合并方式的主异常选择规则不同，调用方须按清理语义选用。
 *
 * @author Reonvia
 * @since 0.4.0
 */
public final class FailureSupport {
    private FailureSupport() {}

    /** 逐层去掉有原因的 {@link CompletionException} 包装。 */
    public static Throwable unwrapCompletionException(Throwable failure) {
        Throwable result = failure;
        while (result instanceof CompletionException && result.getCause() != null) {
            result = result.getCause();
        }
        return result;
    }

    /** 合并两个失败；遇到新的 {@link Error} 时优先以它为主异常。 */
    public static Throwable mergePrioritizingError(Throwable previous, Throwable current) {
        if (previous == null) {
            return current;
        }
        if (current == null || previous == current) {
            return previous;
        }
        if (!(previous instanceof Error) && current instanceof Error) {
            current.addSuppressed(previous);
            return current;
        }
        previous.addSuppressed(current);
        return previous;
    }

    /** 合并两个失败；始终保留首个失败为主异常，即使后来出现 {@link Error}。 */
    public static Throwable mergePreservingFirst(Throwable previous, Throwable current) {
        if (previous == null) {
            return current;
        }
        if (current != null && current != previous) {
            previous.addSuppressed(current);
        }
        return previous;
    }
}
