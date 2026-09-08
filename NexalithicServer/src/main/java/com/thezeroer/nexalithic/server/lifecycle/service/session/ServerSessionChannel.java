package com.thezeroer.nexalithic.server.lifecycle.service.session;

import com.thezeroer.nexalithic.core.io.codec.assembler.PacketsAssembler;
import com.thezeroer.nexalithic.core.io.codec.fragmenter.PacketsFragmenter;
import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;
import com.thezeroer.nexalithic.core.security.SecretKeyContext;
import com.thezeroer.nexalithic.core.session.channel.SessionChannel;
import com.thezeroer.nexalithic.server.lifecycle.service.ServiceLoop;

/**
 * 服务器会话通道
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/09
 * @version 1.0.0
 */
public class ServerSessionChannel<P extends AbstractPacket> extends SessionChannel<P, ServerSession> {
    public record Constant(long MaxIdleTimeNanos) {}
    private final Constant CONSTANT;

    public ServerSessionChannel(AbstractPacket.PacketType packetType, ServerSession session, ServiceLoop<P> loop, PacketsFragmenter<P> fragmenter,
                                PacketsAssembler<P> assembler, SecretKeyContext context, Constant constant) {
        super(packetType, session, loop, fragmenter, assembler, context);
        CONSTANT = constant;
    }

    public long getExpiryTimeNanos() {
        return lastActiveTimeNanos + CONSTANT.MaxIdleTimeNanos;
    }
}
