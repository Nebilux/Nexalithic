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

package com.nebilux.nexalithic.core.model.packet.signaling;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
/**
 * 裸信号
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BareSignal extends SignalingPacket {
    public static final BareSignal HeartBeat = new BareSignal(Signal.HeartBeat);

    private static final BareSignal[] LOOKUP = new BareSignal[256];
    static {
        register(HeartBeat);
    }

    private static void register(BareSignal instance) {
        LOOKUP[instance.getSignal() & 0xFF] = instance;
    }
    private BareSignal(byte signal) {
        super(signal);
        length = 0;
    }

    /**
     * 自动查找单例
     * @return 如果不是 BareSignal 类型则返回 null
     */
    public static BareSignal find(byte signal) {
        return LOOKUP[signal & 0xFF];
    }

    @Override
    protected void onToBuffer(LoopBuffer buffer) {
    }

    @Override
    public short getLength() {
        return 0;
    }
}
