package com.nebilux.nexalithic.core.io.codec.assembler;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 信令包汇编器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/10
 * @version 1.0.0
 */
public class SignalingPacketsAssembler implements PacketsAssembler<SignalingPacket> {
    private static final Logger logger = LoggerFactory.getLogger(SignalingPacketsAssembler.class);
    private final SpscArrayQueue<SignalingPacket> packets = new SpscArrayQueue<>(128);
    private SignalingPacket pendingPacket;

    @Override
    public boolean feed(LoopBuffer source) {
        int flag = source.readableBytes();
        if (pendingPacket != null) {
            if (packets.offer(pendingPacket)) {
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
            if (logger.isTraceEnabled()) {
                logger.trace("received SIGNALING packet [{}]", packet);
            }
            if (packets.offer(packet)) {
                source.dropMarkHead();
            } else {
                this.pendingPacket = packet;
                break;
            }
        }
        return flag != source.readableBytes();
    }

    @Override
    public SignalingPacket drain() {
        if (pendingPacket != null && packets.offer(pendingPacket)) {
            pendingPacket = null;
        }
        return packets.poll();
    }

    @Override
    public void clear() {
        packets.clear();
        pendingPacket = null;
    }
}
