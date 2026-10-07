package com.nebilux.nexalithic.server.session.access;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.model.packet.signaling.channel.ChannelAccessResponseSignal;
import com.nebilux.nexalithic.core.session.SessionKey;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.routing.NetworkRouter;
import com.nebilux.nexalithic.server.session.ServerSession;
import com.nebilux.nexalithic.server.session.SessionRegistry;

import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.Objects;

/**
 * 动态通道访问协调器。
 *
 * <p>负责校验可动态申请的通道类型、选择客户端可访问的服务端路由、生成并登记一次性
 * 通道令牌，以及把访问响应提交到目标会话的信令通道。响应提交失败或提交过程抛出异常时，
 * 已登记的令牌会被撤销，避免遗留永远无法被消费的授权。</p>
 *
 * <p>该组件不负责决定失败后的连接处置方式。调用方可根据 {@link Result} 判断应当断开
 * 信令通道、重试，或仅结束本次业务通道打开操作。</p>
 *
 * @author Reonvia
 * @since 0.3.0
 */
public class ChannelAccessCoordinator {
    /** 一次通道访问授权的处理结果。 */
    public enum Result {
        /** 响应已进入信令发送队列，令牌授权继续有效。 */
        ISSUED,
        /** 当前通道类型不允许通过信令动态申请。 */
        UNSUPPORTED_KIND,
        /** 没有与客户端地址及通道类型匹配的可用路由。 */
        ROUTE_UNAVAILABLE,
        /** 信令发送队列无法接收响应，已登记的令牌已经撤销。 */
        SIGNALING_QUEUE_FULL
    }

    private final SessionRegistry sessionRegistry;
    private final NetworkRouter networkRouter;
    private final SecureRandom secureRandom;

    public ChannelAccessCoordinator(NexalithicBuilderContext context) {
        this.sessionRegistry = context.getModule(NexalithicServer.MODULES.SessionRegistry);
        this.networkRouter = context.getModule(NexalithicServer.MODULES.NetworkRouter);
        this.secureRandom = new SecureRandom();
    }

    /**
     * 为目标会话签发一次指定类型的动态通道访问授权。
     *
     * <p>{@link Result#ISSUED} 只表示访问响应已经进入信令发送队列；客户端仍需使用响应中的
     * 一次性令牌建立并完成对应通道的握手。</p>
     *
     * @param session 令牌绑定的目标会话
     * @param kind 申请的通道类型
     * @param remoteAddress 对端地址，用于选择客户端可访问的路由
     * @return 本次授权处理结果
     */
    public Result issue(ServerSession session, NexalithicChannel.Kind kind, InetAddress remoteAddress) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(remoteAddress, "remoteAddress");
        if (kind != NexalithicChannel.Kind.Packet_Business) {
            return Result.UNSUPPORTED_KIND;
        }
        NetworkRouter.RouteTarget target = networkRouter.chooseTarget(kind, remoteAddress);
        if (target == null) {
            return Result.ROUTE_UNAVAILABLE;
        }
        SessionKey.Immutable token;
        do {
            token = new SessionKey.Immutable(secureRandom.nextLong(), secureRandom.nextLong());
        } while (!sessionRegistry.relateChannelToken(token, session, kind));
        boolean issued = false;
        try {
            issued = session.pushSignalingPacket(new ChannelAccessResponseSignal(kind, target.explicitAddress(), target.port(), token));
            return issued ? Result.ISSUED : Result.SIGNALING_QUEUE_FULL;
        } finally {
            if (!issued) {
                sessionRegistry.removeChannelToken(token, session);
            }
        }
    }
}
