package com.nebilux.nexalithic.core.io.codec.fragmenter;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;

import java.io.IOException;

/**
 * 包分片器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface PacketsFragmenter<P extends AbstractPacket> {
    boolean feed(P packet);
    boolean drain(LoopBuffer target) throws IOException;
    boolean isEmpty();
    void clear();
}
