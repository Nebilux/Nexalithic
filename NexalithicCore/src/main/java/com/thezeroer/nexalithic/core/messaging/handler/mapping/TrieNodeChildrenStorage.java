package com.thezeroer.nexalithic.core.messaging.handler.mapping;

import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;

import java.util.*;

/**
 * Trie 节点的精确匹配子节点存储接口。
 *
 * <p>该接口只管理以具体 {@code short} 路径值索引的精确匹配分支；
 * 通配符分支由 {@link HandlerRegistry.TrieNode#wildcard()} 单独保存。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/16
 * @version 1.0.0
 */
public interface TrieNodeChildrenStorage<HC extends HandlerContext<?>> {
    /**
     * 获取指定路径值对应的子节点。
     *
     * @param key 当前路径层级的精确值
     * @return 对应子节点；不存在时返回 {@code null}
     */
    HandlerRegistry.TrieNode<HC> get(short key);

    /**
     * 保存指定路径值对应的子节点。
     *
     * @param key 当前路径层级的精确值
     * @param node 子节点
     */
    void put(short key, HandlerRegistry.TrieNode<HC> node);

    /**
     * 返回所有精确匹配子节点。
     *
     * @return 子节点集合
     */
    Collection<HandlerRegistry.TrieNode<HC>> all();
}
