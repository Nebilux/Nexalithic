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

package com.nebilux.nexalithic.core.model.stream;

/**
 * 流交付给上层的最小完整单位。
 *
 * <p>序号用于识别块的顺序或缺失。底层可以分片传输一个块，但不得向上层
 * 交付尚未重组完成的块。媒体时间戳、文件偏移及载荷由具体子类定义。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class AbstractChunk {
    private final long sequence;

    protected AbstractChunk(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must be non-negative");
        }
        this.sequence = sequence;
    }

    public final long sequence() {
        return sequence;
    }
}
