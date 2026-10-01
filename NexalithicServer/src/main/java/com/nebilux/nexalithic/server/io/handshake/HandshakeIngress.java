package com.nebilux.nexalithic.server.io.handshake;

import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;

import java.nio.channels.SocketChannel;

/**
 * 握手入口
 *
 * @author Reonvia
 * @since 0.2.0
 */
public interface HandshakeIngress {
    boolean submit(NexalithicChannel.Kind kind, SocketChannel channel);
}
