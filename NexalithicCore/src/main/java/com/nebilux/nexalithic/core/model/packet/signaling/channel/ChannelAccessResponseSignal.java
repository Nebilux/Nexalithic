package com.nebilux.nexalithic.core.model.packet.signaling.channel;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.session.SessionKey;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * 指定 Channel 类型的一次性接入授权响应。
 *
 * <p>一个响应原子地携带 Channel 类型、目标地址、端口和一次性令牌。目标地址为空时，
 * 客户端使用调用方提供的默认地址；这既适用于普通单机部署，也避免向客户端发布
 * {@code 0.0.0.0} 或 {@code ::} 等不可连接的监听地址。</p>
 *
 * @author Reonvia
 * @since 0.3.0
 */
public final class ChannelAccessResponseSignal extends ChannelAccessSignal {
    private static final int DEFAULT_ADDRESS_LENGTH = 0;
    private static final int IPV4_ADDRESS_LENGTH = 4;
    private static final int IPV6_ADDRESS_LENGTH = 16;
    private static final int BASE_CONTENT_LENGTH = KIND_LENGTH + Byte.BYTES + Integer.BYTES + SessionKey.LENGTH;
    private static final int MAXIMUM_CONTENT_LENGTH = BASE_CONTENT_LENGTH + IPV6_ADDRESS_LENGTH;

    private final InetAddress address;
    private final int port;
    private final SessionKey.Immutable token;

    /**
     * @param kind    获得接入授权的 Channel 类型
     * @param address 显式发布的目标地址；为 {@code null} 时对端复用默认地址
     * @param port    目标端口
     * @param token   一次性通道令牌
     */
    public ChannelAccessResponseSignal(NexalithicChannel.Kind kind, InetAddress address, int port, SessionKey token) {
        super(Signal.ChannelAccess_Response, kind, BASE_CONTENT_LENGTH + (address == null ? DEFAULT_ADDRESS_LENGTH : address.getAddress().length));
        validateAddress(address);
        validatePort(port);
        Objects.requireNonNull(token, "token");
        this.address = address;
        this.port = port;
        this.token = new SessionKey.Immutable(token.high(), token.low());
    }

    private ChannelAccessResponseSignal(LoopBuffer buffer, int contentLength) {
        super(Signal.ChannelAccess_Response, buffer, contentLength, BASE_CONTENT_LENGTH, MAXIMUM_CONTENT_LENGTH);
        int encodedAddressLength = buffer.unsafeGetByte();
        if (encodedAddressLength != DEFAULT_ADDRESS_LENGTH && encodedAddressLength != IPV4_ADDRESS_LENGTH && encodedAddressLength != IPV6_ADDRESS_LENGTH) {
            throw new IllegalArgumentException("Invalid channel access address length: " + encodedAddressLength);
        }
        int expectedLength = BASE_CONTENT_LENGTH + encodedAddressLength;
        if (contentLength != expectedLength) {
            throw new IllegalArgumentException(
                    "Invalid channel access response length: " + contentLength + ", expected: " + expectedLength
            );
        }
        this.port = buffer.unsafeGetInt();
        validatePort(port);
        if (encodedAddressLength == DEFAULT_ADDRESS_LENGTH) {
            this.address = null;
        } else {
            byte[] rawAddress = new byte[encodedAddressLength];
            buffer.unsafeGetBytes(rawAddress, encodedAddressLength);
            try {
                this.address = InetAddress.getByAddress(rawAddress);
            } catch (UnknownHostException impossible) {
                throw new IllegalArgumentException("Invalid channel access address", impossible);
            }
            validateAddress(address);
        }
        this.token = new SessionKey.Immutable(buffer.unsafeGetLong(), buffer.unsafeGetLong());
    }

    /** 由 SignalingPacket 解析器创建响应信号。 */
    public static ChannelAccessResponseSignal fromBuffer(LoopBuffer buffer, int contentLength) {
        return new ChannelAccessResponseSignal(buffer, contentLength);
    }

    /** 返回显式地址；使用默认地址时返回 {@code null}。 */
    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public SessionKey.Immutable getToken() {
        return token;
    }

    /** 使用显式地址或默认地址解析客户端实际需要连接的目标。 */
    public InetSocketAddress resolve(InetAddress defaultAddress) {
        return new InetSocketAddress(address != null ? address : Objects.requireNonNull(defaultAddress, "defaultAddress"), port);
    }

    @Override
    public String toString() {
        return super.toString()
                + ", Address: " + (address == null ? "DEFAULT" : address.getHostAddress())
                + ", Port: " + port;
    }

    @Override
    protected void writePayload(LoopBuffer buffer) {
        byte[] rawAddress = address == null ? null : address.getAddress();
        buffer.unsafePut((byte) (rawAddress == null ? DEFAULT_ADDRESS_LENGTH : rawAddress.length));
        buffer.unsafePut(port);
        if (rawAddress != null) {
            buffer.unsafePut(rawAddress, rawAddress.length);
        }
        buffer.unsafePut(token.high());
        buffer.unsafePut(token.low());
    }

    private static void validateAddress(InetAddress address) {
        if (address == null) {
            return;
        }
        int length = address.getAddress().length;
        if (length != IPV4_ADDRESS_LENGTH && length != IPV6_ADDRESS_LENGTH) {
            throw new IllegalArgumentException("Unsupported channel access address length: " + length);
        }
        if (address.isAnyLocalAddress()) {
            throw new IllegalArgumentException("Wildcard address cannot be advertised to a client");
        }
    }

    private static void validatePort(int port) {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid channel access port: " + port);
        }
    }
}
