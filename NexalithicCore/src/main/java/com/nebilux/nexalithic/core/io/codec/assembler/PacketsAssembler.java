package com.nebilux.nexalithic.core.io.codec.assembler;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;

import java.io.IOException;

/**
 * 分组汇编器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface PacketsAssembler<P extends AbstractPacket> {
    boolean feed(LoopBuffer source) throws IOException;
    P drain();
    void clear();
}
