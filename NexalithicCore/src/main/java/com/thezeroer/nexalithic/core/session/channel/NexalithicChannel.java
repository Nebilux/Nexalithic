package com.thezeroer.nexalithic.core.session.channel;

/**
 * Nexalithic 通道
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/07
 * @version 1.0.0
 */
public interface NexalithicChannel {
    enum State {
        Unconnected,
        Connecting,
        Connected,
        Closing,
        Closed,
    }

    void updateLastActiveTimeNanos(long lastActiveTimeNanos);
    long getLastActiveTimeNanos();
    boolean closeChannel();
}
