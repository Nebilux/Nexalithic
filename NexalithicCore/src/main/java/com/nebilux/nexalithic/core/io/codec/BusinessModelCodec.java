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

package com.nebilux.nexalithic.core.io.codec;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务模型编解码器
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class BusinessModelCodec extends NexalithicModelCodec {
    protected static final int COMMON_BASE_HEADER_SIZE = Long.BYTES // correlationId
            + Short.BYTES // Way
            + Byte.BYTES; // pathDepth
    protected static final int MIN_HEADER_COUNT = 2;

    protected final AtomicInteger outstandingContextCount = new AtomicInteger();
    protected final BusinessModelCodecCallback.Factory codecCallbackFactory;

    protected BusinessModelCodec(int maxOutstandingContextCount, BusinessModelCodecCallback.Factory codecCallbackFactory) {
        super(maxOutstandingContextCount);
        this.codecCallbackFactory = codecCallbackFactory;
    }

    protected static class Context {
        private final int modelSequence;

        protected Context(int modelSequence) {
            this.modelSequence = modelSequence;
        }

        public final int modelSequence() {
            return modelSequence;
        }
    }
}
