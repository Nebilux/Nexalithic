package com.thezeroer.nexalithic.core.io.loop;

import com.thezeroer.nexalithic.core.builder.NexalithicBuilderContext;
import com.thezeroer.nexalithic.core.session.SessionChannel;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 会话循环
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/12
 */
public abstract class SessionLoop<H extends ChannelLoop.Handoff,C extends SessionChannel<?, ?>> extends ChannelLoop<H, C> {
    public SessionLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
    }

    @Override
    protected void onSelectionKeyMigrated(SelectionKey oldKey, SelectionKey newKey) {
        if (oldKey.attachment() instanceof SessionChannel<?, ?> channel) {
            channel.replaceSelectionKey(newKey);
        }
    }

    public void postRateUpdate(C channel) {}
}
