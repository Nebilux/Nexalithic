package com.nebilux.nexalithic.core.messaging.handler.mapping;

import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于映射表的 Trie 精确子节点存储。
 *
 * <p>适用于路径值分布稀疏或范围无法预知的场景。相比数组存储，
 * 该实现避免为未使用的 key 预留空间。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class MapTrieNodeChildrenStorage<HC extends HandlerContext<?>> implements TrieNodeChildrenStorage<HC> {
    private final Map<Short, HandlerRegistry.TrieNode<HC>> map = new HashMap<>();

    @Override
    public HandlerRegistry.TrieNode<HC> get(short key) {
        return map.get(key);
    }

    @Override
    public void put(short key, HandlerRegistry.TrieNode<HC> node) {
        map.put(key, node);
    }

    @Override
    public Collection<HandlerRegistry.TrieNode<HC>> all() {
        return map.values();
    }
}
