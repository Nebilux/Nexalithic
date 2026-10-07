package com.nebilux.nexalithic.server.routing;

import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于客户端 CIDR、显式默认路由和活动监听器选择对外发布的通道接入地址。
 *
 * <p>查询优先级固定为：CIDR 路由、显式默认路由、自动监听路由。路由表和自动监听器均采用
 * 写时复制快照；写入由同步方法串行化，高频查询不需要获取锁。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class NetworkRouter {
    private static final Comparator<RouteEntry> ENTRY_COMPARATOR = Comparator.comparing(RouteEntry::getStart).thenComparing(RouteEntry::getEnd, Comparator.reverseOrder());
    private final Map<NexalithicChannel.Kind, RouteTable> routingTables = new EnumMap<>(NexalithicChannel.Kind.class);

    public NetworkRouter() {
        for (NexalithicChannel.Kind kind : NexalithicChannel.Kind.values()) {
            routingTables.put(kind, new RouteTable());
        }
    }

    /** 添加使用显式发布地址的 CIDR 路由。 */
    public synchronized void addRoute(NexalithicChannel.Kind kind, String cidr, InetAddress advertisedAddress, int port) {
        addRoute(kind, new RouteEntry(cidr, RouteTarget.explicitAddress(advertisedAddress, port)));
    }

    /** 添加使用“默认地址 + 指定端口”的 CIDR 路由。 */
    public synchronized void addRoute(NexalithicChannel.Kind kind, String cidr, int port) {
        addRoute(kind, new RouteEntry(cidr, RouteTarget.defaultAddress(port)));
    }

    /** 添加已经构造完成的 CIDR 路由。 */
    public synchronized void addRoute(NexalithicChannel.Kind kind, RouteEntry route) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(route, "route");
        RouteTable table = routingTables.get(kind);
        if (route.isV4()) {
            table.v4 = appendAndSort(table.v4, route);
        } else {
            table.v6 = appendAndSort(table.v6, route);
        }
    }

    /** 批量添加 CIDR 路由。一次完成数组复制和排序，适合初始化大量规则。 */
    public synchronized void addRoutes(NexalithicChannel.Kind kind, RouteEntry... routes) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(routes, "routes");
        RouteTable table = routingTables.get(kind);
        List<RouteEntry> v4 = new ArrayList<>(Arrays.asList(table.v4));
        List<RouteEntry> v6 = new ArrayList<>(Arrays.asList(table.v6));
        for (RouteEntry route : routes) {
            Objects.requireNonNull(route, "route");
            (route.isV4() ? v4 : v6).add(route);
        }
        v4.sort(ENTRY_COMPARATOR);
        v6.sort(ENTRY_COMPARATOR);
        table.v4 = v4.toArray(RouteEntry[]::new);
        table.v6 = v6.toArray(RouteEntry[]::new);
    }

    /** 移除与指定 CIDR 完全相同的全部路由。 */
    public synchronized void removeRoute(NexalithicChannel.Kind kind, String cidr) {
        Objects.requireNonNull(kind, "kind");
        RouteEntry target = new RouteEntry(cidr, RouteTarget.defaultAddress(1));
        RouteTable table = routingTables.get(kind);
        if (target.isV4()) {
            table.v4 = Arrays.stream(table.v4)
                    .filter(route -> !route.sameNetwork(target))
                    .toArray(RouteEntry[]::new);
        } else {
            table.v6 = Arrays.stream(table.v6)
                    .filter(route -> !route.sameNetwork(target))
                    .toArray(RouteEntry[]::new);
        }
    }

    /** 设置未命中 CIDR 时优先使用的显式默认路由；它覆盖自动监听路由。 */
    public synchronized void setDefaultRoute(NexalithicChannel.Kind kind, RouteTarget target) {
        routingTables.get(Objects.requireNonNull(kind, "kind")).defaultRoute = Objects.requireNonNull(target, "target");
    }

    /** 清除显式默认路由，使查询重新回退到活动监听器。 */
    public synchronized void clearDefaultRoute(NexalithicChannel.Kind kind) {
        routingTables.get(Objects.requireNonNull(kind, "kind")).defaultRoute = null;
    }

    /**
     * 注册一个已经成功接入 Selector 的本地监听端口。
     *
     * <p>自动路由只发布端口，地址由客户端使用调用方提供的默认地址解析。因此即使服务端
     * 绑定通配地址，也不会把不可连接的通配地址发给客户端。</p>
     *
     * @return 与该监听器一一对应的注册句柄；关闭句柄即自动注销路由
     */
    public RouteRegistration registerLocalEndpoint(NexalithicChannel.Kind kind, int port) {
        return registerLocalEndpoint(kind, RouteTarget.defaultAddress(port));
    }

    /** 注册一个监听器关联的自动路由目标。 */
    public synchronized RouteRegistration registerLocalEndpoint(NexalithicChannel.Kind kind, RouteTarget target) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(target, "target");
        RouteTable table = routingTables.get(kind);
        LocalRegistration registration = new LocalRegistration(this, kind, target);
        LocalRegistration[] previous = table.localRoutes;
        LocalRegistration[] updated = Arrays.copyOf(previous, previous.length + 1);
        updated[previous.length] = registration;
        table.localRoutes = updated;
        return registration;
    }

    /**
     * 为指定客户端选择接入目标。
     *
     * @return CIDR、默认路由或活动监听器目标；没有可用目标时返回 {@code null}
     */
    public RouteTarget chooseTarget(NexalithicChannel.Kind kind, InetAddress remoteAddress) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(remoteAddress, "remoteAddress");
        RouteTable table = routingTables.get(kind);
        byte[] rawAddress = remoteAddress.getAddress();
        RouteTarget matched = findRoute(
                rawAddress.length == 4 ? table.v4 : table.v6,
                new BigInteger(1, rawAddress)
        );
        if (matched != null) {
            return matched;
        }
        RouteTarget defaultRoute = table.defaultRoute;
        if (defaultRoute != null) {
            return defaultRoute;
        }
        LocalRegistration[] localRoutes = table.localRoutes;
        if (localRoutes.length == 0) {
            return null;
        }
        int index = Math.floorMod(table.localCursor.getAndIncrement(), localRoutes.length);
        return localRoutes[index].target();
    }

    private synchronized void unregisterLocalEndpoint(LocalRegistration registration) {
        RouteTable table = routingTables.get(registration.kind);
        LocalRegistration[] previous = table.localRoutes;
        for (int index = 0; index < previous.length; index++) {
            if (previous[index] != registration) {
                continue;
            }
            LocalRegistration[] updated = new LocalRegistration[previous.length - 1];
            System.arraycopy(previous, 0, updated, 0, index);
            System.arraycopy(previous, index + 1, updated, index, previous.length - index - 1);
            table.localRoutes = updated;
            return;
        }
    }

    private static RouteEntry[] appendAndSort(RouteEntry[] routes, RouteEntry route) {
        RouteEntry[] updated = Arrays.copyOf(routes, routes.length + 1);
        updated[routes.length] = route;
        Arrays.sort(updated, ENTRY_COMPARATOR);
        return updated;
    }

    /**
     * 返回起始地址不大于目标地址的最后一条规则，再向前寻找第一个覆盖目标的 CIDR。
     * 同起始地址下更窄的网段排在后面，因此自然优先于更宽的网段。
     */
    private static RouteTarget findRoute(RouteEntry[] routes, BigInteger address) {
        int low = 0;
        int high = routes.length - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            if (routes[middle].start.compareTo(address) <= 0) {
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        for (int index = high; index >= 0; index--) {
            RouteEntry route = routes[index];
            if (address.compareTo(route.end) <= 0) {
                return route.target;
            }
        }
        return null;
    }

    /** 一次路由选择的完整结果。 */
    public static final class RouteTarget {
        private sealed interface AddressTarget permits DefaultAddress, ExplicitAddress {}

        private enum DefaultAddress implements AddressTarget {
            INSTANCE
        }

        private record ExplicitAddress(InetAddress address) implements AddressTarget {
            private ExplicitAddress {
                Objects.requireNonNull(address, "address");
                if (address.isAnyLocalAddress()) {
                    throw new IllegalArgumentException("Wildcard address cannot be advertised to a client");
                }
            }
        }

        private final AddressTarget addressTarget;
        private final int port;

        private RouteTarget(AddressTarget addressTarget, int port) {
            this.addressTarget = Objects.requireNonNull(addressTarget, "addressTarget");
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Invalid route port: " + port);
            }
            this.port = port;
        }

        public static RouteTarget defaultAddress(int port) {
            return new RouteTarget(DefaultAddress.INSTANCE, port);
        }

        public static RouteTarget explicitAddress(InetAddress address, int port) {
            return new RouteTarget(new ExplicitAddress(address), port);
        }

        /** 返回显式地址；使用默认地址时返回 {@code null}。 */
        public InetAddress explicitAddress() {
            return addressTarget instanceof ExplicitAddress(InetAddress address) ? address : null;
        }

        public int port() {
            return port;
        }

        @Override
        public boolean equals(Object object) {
            return this == object
                    || object instanceof RouteTarget other
                    && port == other.port
                    && addressTarget.equals(other.addressTarget);
        }

        @Override
        public int hashCode() {
            return Objects.hash(addressTarget, port);
        }

        @Override
        public String toString() {
            return "RouteTarget[addressTarget=" + addressTarget + ", port=" + port + ']';
        }
    }

    /** CIDR 路由条目。 */
    public static class RouteEntry implements Comparable<BigInteger> {
        private final BigInteger start;
        private final BigInteger end;
        private final int prefixLength;
        private final RouteTarget target;
        private final boolean isV4;

        public RouteEntry(String cidr, RouteTarget target) {
            Objects.requireNonNull(cidr, "cidr");
            this.target = Objects.requireNonNull(target, "target");
            String[] parts = cidr.split("/", -1);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid CIDR: " + cidr);
            }
            InetAddress address;
            try {
                address = InetAddress.getByName(parts[0]);
            } catch (UnknownHostException failure) {
                throw new IllegalArgumentException("Invalid CIDR address: " + cidr, failure);
            }
            byte[] rawAddress = address.getAddress();
            int totalBits = rawAddress.length * Byte.SIZE;
            try {
                prefixLength = Integer.parseInt(parts[1]);
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException("Invalid CIDR prefix: " + cidr, failure);
            }
            if (prefixLength < 0 || prefixLength > totalBits) {
                throw new IllegalArgumentException("Invalid CIDR prefix: " + cidr);
            }
            BigInteger addressValue = new BigInteger(1, rawAddress);
            int hostBits = totalBits - prefixLength;
            start = addressValue.shiftRight(hostBits).shiftLeft(hostBits);
            end = start.add(BigInteger.ONE.shiftLeft(hostBits).subtract(BigInteger.ONE));
            isV4 = rawAddress.length == 4;
        }

        @Override
        public int compareTo(BigInteger address) {
            if (address.compareTo(start) < 0) {
                return 1;
            }
            if (address.compareTo(end) > 0) {
                return -1;
            }
            return 0;
        }

        public boolean isV4() {
            return isV4;
        }

        public int getPort() {
            return target.port();
        }

        public RouteTarget getTarget() {
            return target;
        }

        public BigInteger getStart() {
            return start;
        }

        public BigInteger getEnd() {
            return end;
        }

        private boolean sameNetwork(RouteEntry other) {
            return isV4 == other.isV4 && prefixLength == other.prefixLength && start.equals(other.start);
        }
    }

    private static final class RouteTable {
        private volatile RouteEntry[] v4 = new RouteEntry[0];
        private volatile RouteEntry[] v6 = new RouteEntry[0];
        private volatile RouteTarget defaultRoute;
        private volatile LocalRegistration[] localRoutes = new LocalRegistration[0];
        private final AtomicInteger localCursor = new AtomicInteger();
    }

    /** 活动监听路由的生命周期句柄。 */
    public interface RouteRegistration extends AutoCloseable {
        RouteTarget target();
        boolean isActive();
        @Override
        void close();
    }

    private static final class LocalRegistration implements RouteRegistration {
        private final NetworkRouter owner;
        private final NexalithicChannel.Kind kind;
        private final RouteTarget target;
        private final AtomicBoolean active = new AtomicBoolean(true);

        private LocalRegistration(NetworkRouter owner, NexalithicChannel.Kind kind, RouteTarget target) {
            this.owner = owner;
            this.kind = kind;
            this.target = target;
        }

        @Override
        public RouteTarget target() {
            return target;
        }

        @Override
        public boolean isActive() {
            return active.get();
        }

        @Override
        public void close() {
            if (active.compareAndSet(true, false)) {
                owner.unregisterLocalEndpoint(this);
            }
        }
    }
}
