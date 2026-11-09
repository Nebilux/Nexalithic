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

package com.nebilux.nexalithic.core.model.business.packet.codec;

import com.nebilux.nexalithic.core.model.business.codec.CodecRegistry;
import com.nebilux.nexalithic.core.model.business.codec.CodecStorage;

/**
 * Payload 编解码器的只读注册表。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class PartCodecRegistry extends CodecRegistry<PartCodec<?>> {
    private PartCodecRegistry(CodecStorage<PartCodec<?>> storage) {
        super(storage);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends CodecRegistry.Builder<PartCodec<?>, Builder> {
        private Builder() {}

        @Override
        protected Builder self() {
            return this;
        }

        public PartCodecRegistry build() {
            return new PartCodecRegistry(buildStorage());
        }
    }
}
