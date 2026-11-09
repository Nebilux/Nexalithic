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

import com.nebilux.nexalithic.core.model.business.codec.PayloadCodec;
import com.nebilux.nexalithic.core.model.business.packet.BusinessPacket;

import java.io.IOException;

/**
 * 业务包附加项的线路类型及分段编解码器工厂。每次编码或解码分别创建独立的状态对象。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public interface PartCodec<P extends BusinessPacket.Part<?>> extends PayloadCodec<P> {
    Encoder<P> encoder();

    Decoder<P> decoder();

    interface Encoder<P extends BusinessPacket.Part<?>> extends PayloadCodec.Encoder {
        /** 绑定模型并返回完整编码长度。 */
        long prepare(P part) throws IOException;
    }

    interface Decoder<P extends BusinessPacket.Part<?>> extends PayloadCodec.Decoder<P> {
        void prepare(long size) throws IOException;
    }
}
