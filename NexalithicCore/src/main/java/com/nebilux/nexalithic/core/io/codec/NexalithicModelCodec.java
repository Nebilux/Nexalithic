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

import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.connection.session.SessionKey;

/**
 * 矩阵模型编解码器
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class NexalithicModelCodec {
    protected static abstract class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> MaxOutstandingContextCount = defineOption(
                MaxOutstandingContextCount_Value(), OptionValidator.positive()
        );
        protected Options(Class<?> holder) {
            super(holder);
        }
        protected abstract int MaxOutstandingContextCount_Value();
    }

    protected final SessionKey sessionKey;
    protected final int maxOutstandingContextCount;

    protected NexalithicModelCodec(SessionKey sessionKey, int maxOutstandingContextCount) {
        if (maxOutstandingContextCount <= 0) {
            throw new IllegalArgumentException("maxOutstandingContextCount must be positive");
        }
        this.sessionKey = sessionKey;
        this.maxOutstandingContextCount = maxOutstandingContextCount;
    }
}
