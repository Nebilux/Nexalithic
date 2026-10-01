package com.nebilux.nexalithic.server.manager;

import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * <h1>网络路由器 (NetworkRouter)</h1>
 * <p>基于 CIDR 网段的高性能双栈路由器。采用静态数组 + 二分查找算法实现，旨在提供极低的查询延迟。</p>
 * <h2>核心特性：</h2>
 * <ul>
 * <li><b>高性能查询：</b> 利用数组内存连续性及二分查找，实现 O(log N) 检索效率。</li>
 * <li><b>双栈支持：</b> 物理隔离 IPv4 与 IPv6 路由表，避免 IPv6 检索干扰 IPv4 性能。</li>
 * <li><b>并发安全：</b> 写入端通过 synchronized 同步，读取端无锁（volatile 引用切换），实现写时复制 (COW) 模式。</li>
 * </ul>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class NetworkRouter {
    private final Map<NexalithicChannel.Kind, RouteTable> routingTables = new EnumMap<>(NexalithicChannel.Kind.class);

    /**
     * 添加单条路由规则。
     * <p>注意：由于涉及数组拷贝与重排序，在高频批量添加场景下建议使用 {@link #addRoutes(NexalithicChannel.Kind, RouteEntry...)}。</p>
     *
     * @param kind 数据协议类型
     * @param cidr CIDR 格式网段 (如: 192.168.1.0/24)
     * @param port 目标后端端口
     * @throws UnknownHostException 当 CIDR 地址解析失败时抛出
     */
    public synchronized void addRoute(NexalithicChannel.Kind kind, String cidr, int port) throws UnknownHostException {
        RouteTable table = routingTables.computeIfAbsent(kind, k -> new RouteTable());
        RouteEntry entry = new RouteEntry(cidr, port);
        RouteEntry[] oldArray = entry.isV4() ? table.v4 : table.v6;
        RouteEntry[] newArray = Arrays.copyOf(oldArray, oldArray.length + 1);
        newArray[oldArray.length] = entry;
        Arrays.sort(newArray, Comparator.comparing(e -> e.start));
        if (entry.isV4()) {
            table.v4 = newArray;
        } else {
            table.v6 = newArray;
        }
    }

    /**
     * 批量添加路由规则。
     * <p>采用统一排序策略，相比单条循环添加能显著减少 CPU 排序开销。排序逻辑遵循起始地址升序，同地址下掩码越长（范围越小）越靠后。</p>
     *
     * @param kind 数据协议类型
     * @param routes 路由条目变长数组
     */
    public synchronized void addRoutes(NexalithicChannel.Kind kind, RouteEntry... routes) {
        RouteTable table = routingTables.computeIfAbsent(kind, k -> new RouteTable());
        List<RouteEntry> v4List = new ArrayList<>(Arrays.asList(table.v4));
        List<RouteEntry> v6List = new ArrayList<>(Arrays.asList(table.v6));
        for (RouteEntry entry : routes) {
            if (entry.isV4()) {
                v4List.add(entry);
            } else {
                v6List.add(entry);
            }
        }
        Comparator<RouteEntry> entryComparator = Comparator.comparing((RouteEntry e) -> e.start).thenComparing(e -> e.end);
        v4List.sort(entryComparator);
        v6List.sort(entryComparator);
        table.v4 = v4List.toArray(new RouteEntry[0]);
        table.v6 = v6List.toArray(new RouteEntry[0]);
    }

    /**
     * 移除特定网段的路由。
     * <p>采用过滤模式重构数组，移除后数组依然保持原有顺序，无需重新排序。</p>
     *
     * @param kind 数据协议类型
     * @param cidr 待移除的 CIDR 网段
     * @throws UnknownHostException 地址解析失败
     */
    public synchronized void removeRoute(NexalithicChannel.Kind kind, String cidr) throws UnknownHostException {
        RouteTable table = routingTables.get(kind);
        if (table == null) {
            return;
        }
        RouteEntry target = new RouteEntry(cidr, 0);
        if (target.isV4()) {
            table.v4 = Arrays.stream(table.v4)
                    .filter(e -> !e.getStart().equals(target.getStart()))
                    .toArray(RouteEntry[]::new);
        } else {
            table.v6 = Arrays.stream(table.v6)
                    .filter(e -> !e.getStart().equals(target.getStart()))
                    .toArray(RouteEntry[]::new);
        }
    }

    /**
     * 根据当前会话通道信息选择目标端口。
     * <p>这是高频调用路径，通过判断地址长度（4或16）自动分发到对应的 V4/V6 二分查找逻辑。</p>
     *
     * @return 目标端口号，若未匹配到任何路由则返回 -1
     */
    public int choosePort(NexalithicChannel.Kind kind, InetAddress remoteAddress) {
        RouteTable table = routingTables.get(kind);
        if (table == null) {
            return -1;
        }
        byte[] raw = remoteAddress.getAddress();
        return binarySearch((raw.length == 4) ? table.v4 : table.v6, new BigInteger(1, raw));
    }

    private int binarySearch(RouteEntry[] entries, BigInteger ipVal) {
        int low = 0;
        int high = entries.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = entries[mid].compareTo(ipVal);
            if (cmp == 0) return entries[mid].getPort();
            if (cmp < 0) low = mid + 1;
            else high = mid - 1;
        }
        return -1;
    }

    public static class RouteEntry implements Comparable<BigInteger> {
        private final BigInteger start;
        private final BigInteger end;
        private final int port;
        private final boolean isV4;

        public RouteEntry(String cidr, int port) throws UnknownHostException {
            String[] parts = cidr.split("/");
            InetAddress addr = InetAddress.getByName(parts[0]);
            byte[] raw = addr.getAddress();
            int prefix = Integer.parseInt(parts[1]);
            BigInteger ipVal = new BigInteger(1, raw);
            int totalBits = raw.length * 8;
            int shift = totalBits - prefix;
            BigInteger start = ipVal.shiftRight(shift).shiftLeft(shift);
            BigInteger mask = BigInteger.ONE.shiftLeft(shift).subtract(BigInteger.ONE);
            BigInteger end = start.add(mask);
            this.isV4 = raw.length == 4;
            this.start = start;
            this.end = end;
            this.port = port;
        }

        @Override
        public int compareTo(BigInteger ip) {
            if (ip.compareTo(this.start) < 0) return 1;
            if (ip.compareTo(this.end) > 0) return -1;
            return 0;
        }

        public boolean isV4() {
            return isV4;
        }

        public int getPort() {
            return port;
        }

        public BigInteger getStart() {
            return start;
        }
        public BigInteger getEnd() {
            return end;
        }
    }
    private static class RouteTable {
        private volatile RouteEntry[] v4 = new RouteEntry[0];
        private volatile RouteEntry[] v6 = new RouteEntry[0];
    }
}
