package com.nebilux.nexalithic.core.infra.loadbalance;

import java.util.Map;
import java.util.TreeMap;

public class ConsistentHashBalancer<K, V extends LoadBalanceable> implements LoadBalancer<K, V> {
    private final TreeMap<Integer, V> ring = new TreeMap<>();
    private final V[] candidates;

    /**
     * @param candidates        参与负载的单元数组
     * @param virtualNodes 每个物理节点对应的虚拟节点数（建议 160-256）
     */
    public ConsistentHashBalancer(V[] candidates, int virtualNodes) {
        this.candidates = candidates;
        for (V unit : candidates) {
            for (int i = 0; i < virtualNodes; i++) {
                // 混合单元 ID 和索引生成哈希，确保虚拟节点散落在环上
                int hash = hash(unit.toString() + "-VNODE-" + i);
                ring.put(hash, unit);
            }
        }
    }

    @Override
    public V select(K key) {
        if (candidates.length == 1) {
            return candidates[0];
        }
        int hash = hash(key.toString());
        Map.Entry<Integer, V> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            return ring.firstEntry().getValue();
        }
        return entry.getValue();
    }

    @Override
    public V[] all() {
        return candidates;
    }

    private int hash(String key) {
        int h;
        return (key == null) ? 0 : ((h = key.hashCode()) ^ (h >>> 16)) & 0x7fffffff;
    }
}