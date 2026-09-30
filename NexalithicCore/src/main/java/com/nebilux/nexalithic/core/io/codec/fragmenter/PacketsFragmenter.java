package com.nebilux.nexalithic.core.io.codec.fragmenter;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;

import java.io.IOException;

/**
 * 包分片器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/03
 * @version 1.0.0
 */
public interface PacketsFragmenter<P extends AbstractPacket> {
    boolean feed(P packet);
    int fill(P... packets);
    boolean drain(LoopBuffer target) throws IOException;
    boolean isEmpty();
    void clear();
}
