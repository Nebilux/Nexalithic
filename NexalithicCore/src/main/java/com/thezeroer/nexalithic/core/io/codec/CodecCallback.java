package com.thezeroer.nexalithic.core.io.codec;

import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;
import com.thezeroer.nexalithic.core.session.NexalithicSession;

/**
 * 编解码器回调
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/14
 */
public interface CodecCallback {
    void bind(NexalithicSession<?> session);

    void prepare(long taskId, BusinessPacket.Way way);
    void start(long total);
    void update(long remaining);
    void complete();

    void clear();
}
