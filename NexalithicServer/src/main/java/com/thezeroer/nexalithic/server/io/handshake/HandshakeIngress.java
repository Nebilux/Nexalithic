package com.thezeroer.nexalithic.server.io.handshake;

import com.thezeroer.nexalithic.core.io.channel.NexalithicChannel;

import java.nio.channels.SocketChannel;

/**
 * 握手入口
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/26
 */
public interface HandshakeIngress {
    boolean submit(NexalithicChannel.Kind kind, SocketChannel channel);
}
