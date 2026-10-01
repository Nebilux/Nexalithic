package com.nebilux.nexalithic.core.io.codec;

import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;

/**
 * 编解码器回调
 *
 * @author Reonvia
 * @since 0.2.0
 */
public interface CodecCallback {
    void bind(NexalithicSession<?> session);

    void prepare(long taskId, BusinessPacket.Way way);
    void start(long total);
    void update(long remaining);
    void complete();

    void clear();
}
