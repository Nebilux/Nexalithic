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
 * Nexalithic 注册冲突异常。
 *
 * <p>当某个键或标识已被占用、框架不允许新的注册覆盖已有项时使用；
 * 重复键与载荷标识冲突由对应的子类进一步说明。</p>
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class NexalithicConflictException extends NexalithicUncheckedException {
    public NexalithicConflictException(String message) {
        super(message);
    }
}
