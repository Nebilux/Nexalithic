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

package com.nebilux.nexalithic.core.io.codec.assembler;

import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.io.codec.NexalithicModelCodec;
import com.nebilux.nexalithic.core.io.codec.fragmenter.SignalingPacketFragmenter;
import com.nebilux.nexalithic.core.model.signaling.packet.SignalingPacket;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 信令包汇编器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SignalingPacketAssembler extends NexalithicModelCodec implements ModelAssembler<SignalingPacket> {
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

    private static final Logger logger = LoggerFactory.getLogger(SignalingPacketAssembler.class);
    private static final boolean LOGGER_TRACE_ENABLED = logger.isTraceEnabled();

    private final SpscArrayQueue<SignalingPacket> completedPackets;
    private SignalingPacket pendingPacket;

    public SignalingPacketAssembler(int maxOutstandingContextCount) {
        super(maxOutstandingContextCount);
        completedPackets = new SpscArrayQueue<>(maxOutstandingContextCount);
    }

    @Override
    public boolean feed(LoopBuffer source) {
        int initialReadable = source.readableBytes();
        if (pendingPacket != null) {
            if (completedPackets.offer(pendingPacket)) {
                pendingPacket = null;
            } else {
                return false;
            }
        }
        while (true) {
            int readable = source.readableBytes();
            if (readable < SignalingPacket.HEADER_LENGTH) {
                break;
            }
            SignalingPacket packet = SignalingPacket.fromBuffer(source);
            if (packet == null) {
                break;
            }
            if (LOGGER_TRACE_ENABLED) {
                logger.trace("received SIGNALING packet [{}]", packet);
            }
            if (completedPackets.offer(packet)) {
                source.dropMarkHead();
            } else {
                this.pendingPacket = packet;
                break;
            }
        }
        return initialReadable != source.readableBytes();
    }

    @Override
    public SignalingPacket drain() {
        if (pendingPacket != null && completedPackets.offer(pendingPacket)) {
            pendingPacket = null;
        }
        return completedPackets.poll();
    }

    @Override
    public void clear() {
        completedPackets.clear();
        pendingPacket = null;
    }
}
