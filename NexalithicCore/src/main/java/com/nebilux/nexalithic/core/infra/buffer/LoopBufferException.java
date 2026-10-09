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

package com.nebilux.nexalithic.core.infra.buffer;

import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 循环缓冲区读写边界异常的基类。
 *
 * <p>当一次读写超过物理可用空间，或超过有限视图允许的逻辑配额时，
 * 由对应子类指出具体是哪一类边界被越过。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class LoopBufferException extends NexalithicUncheckedException {
    protected LoopBufferException(String message) {
        super(message, true);
    }
}
