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

package com.nebilux.nexalithic.core.session;

import java.nio.ByteBuffer;

/**
 * 会话密钥
 *
 * @author Reonvia
 * @since 0.1.0
 */
public sealed interface SessionKey permits SessionKey.Immutable, SessionKey.Mutable {
    int LENGTH = Long.BYTES * 2;

    long high();
    long low();

    default byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(LENGTH);
        buf.putLong(high());
        buf.putLong(low());
        return buf.array();
    }
    default ByteBuffer toByteBuffer(ByteBuffer buffer) {
        buffer.putLong(high());
        buffer.putLong(low());
        return buffer;
    }

    /**
     * 不可变版本：用于存储在 Map 的 Key 中
     */
    final class Immutable implements SessionKey {
        private final long high, low;
        private final int hashCode;

        public Immutable(long high, long low) {
            this.high = high;
            this.low = low;
            hashCode = 31 * Long.hashCode(high) + Long.hashCode(low);
        }
        public Immutable(ByteBuffer buffer, int offset) {
            this(buffer.getLong(offset), buffer.getLong(offset + Long.BYTES));
        }

        @Override
        public long high() {
            return high;
        }
        @Override
        public long low() {
            return low;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            return obj instanceof SessionKey other
                    && this.high == other.high()
                    && this.low == other.low();
        }
    }

    /**
     * 可变版本：仅限单线程内查询使用（如 ThreadLocal）
     */
    final class Mutable implements SessionKey {
        private long high, low;
        private int hashCode;

        public Mutable wrap(long high, long low) {
            this.high = high;
            this.low = low;
            hashCode = 31 * Long.hashCode(high) + Long.hashCode(low);
            return this;
        }
        public Mutable wrap(ByteBuffer buffer, int offset) {
            return this.wrap(buffer.getLong(offset), buffer.getLong(offset + Long.BYTES));
        }

        @Override
        public long high() {
            return high;
        }
        @Override
        public long low() {
            return low;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            return obj instanceof SessionKey other
                    && this.high == other.high()
                    && this.low == other.low();
        }
    }
}
