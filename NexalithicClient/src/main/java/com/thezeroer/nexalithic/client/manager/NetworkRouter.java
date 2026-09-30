package com.thezeroer.nexalithic.client.manager;

import com.thezeroer.nexalithic.core.model.packet.AbstractPacket;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 网络路由器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/07
 * @version 1.0.0
 */
public class NetworkRouter {
    private volatile InetSocketAddress serverAddress;
    private final Map<AbstractPacket.PacketType, Integer> ports = new ConcurrentHashMap<>();

    public void setServerAddress(InetSocketAddress serverAddress) {
        this.serverAddress = serverAddress;
        ports.put(AbstractPacket.PacketType.Signaling, serverAddress.getPort());
    }
    public InetSocketAddress getServerAddress() {
        return serverAddress;
    }
    public String getServerHost() {
        return serverAddress.getAddress().getHostAddress();
    }

    public void setPort(AbstractPacket.PacketType type, int port) {
        ports.put(type, port);
    }
    public Integer getPort(AbstractPacket.PacketType type) {
        return ports.get(type);
    }

    public void clear() {
        serverAddress = null;
        ports.clear();
    }
}
