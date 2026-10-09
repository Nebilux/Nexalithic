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
/**
 * Nexalithic 受检异常基类。
 *
 * <p>用于构建终端、打开服务器监听器、建立客户端连接等同步操作失败；
 * 具体子类描述失败发生的操作。调用者需要捕获该异常或在方法签名中声明。</p>
 *
 * @author Reonvia
 * @since 0.3.1
 */
public abstract class NexalithicCheckedException extends Exception implements NexalithicFailure {
    protected NexalithicCheckedException(String message) {
        super(message);
    }

    protected NexalithicCheckedException(String message, Throwable cause) {
        super(message, cause);
    }
}
