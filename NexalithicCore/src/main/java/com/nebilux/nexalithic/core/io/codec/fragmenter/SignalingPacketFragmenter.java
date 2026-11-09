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

package com.nebilux.nexalithic.core.io.codec.fragmenter;

import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.io.codec.NexalithicModelCodec;
import com.nebilux.nexalithic.core.model.signaling.packet.SignalingPacket;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 信令分组碎片器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SignalingPacketFragmenter extends NexalithicModelCodec implements ModelFragmenter<SignalingPacket> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, SignalingPacketFragmenter.class);
    public static final class Options extends NexalithicModelCodec.Options {
        private Options(Class<?> holder) {
            super(holder);
        }

        @Override
        protected int MaxOutstandingContextCount_Value() {
            return 128;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(SignalingPacketFragmenter.class);
    private static final boolean LOGGER_TRACE_ENABLED = logger.isTraceEnabled();

    private final MpscArrayQueue<SignalingPacket> incomingPackets;
    private SignalingPacket pendingPacket;

    public SignalingPacketFragmenter(int maxOutstandingContextCount) {
        super(maxOutstandingContextCount);
        incomingPackets = new MpscArrayQueue<>(maxOutstandingContextCount);
    }

    @Override
    public boolean feed(SignalingPacket wrapper) {
        return incomingPackets.offer(wrapper);
    }

    @Override
    public boolean drain(LoopBuffer target) {
        int initialWritable = target.writableBytes();
        for (SignalingPacket packet = takeInitialPacket(); packet != null; packet = incomingPackets.poll()) {
            if (packet.toBuffer(target)) {
                if (LOGGER_TRACE_ENABLED) {
                    logger.trace("SignalingPacketFragmenter drain packet [{}]", packet);
                }
            } else {
                pendingPacket = packet;
                break;
            }
        }
        return initialWritable != target.writableBytes();
    }

    @Override
    public boolean isEmpty() {
        return pendingPacket == null && incomingPackets.isEmpty();
    }

    @Override
    public void clear() {
        incomingPackets.clear();
        pendingPacket = null;
    }

    private SignalingPacket takeInitialPacket() {
        SignalingPacket result = pendingPacket;
        pendingPacket = null;
        return result != null ? result : incomingPackets.poll();
    }
}
