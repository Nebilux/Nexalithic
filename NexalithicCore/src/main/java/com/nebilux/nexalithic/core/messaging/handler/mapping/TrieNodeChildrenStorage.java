/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.messaging.handler.mapping;

import com.nebilux.nexalithic.core.messaging.handler.HandlerContext;

import java.util.*;

/**
 * Trie 节点的精确匹配子节点存储接口。
 *
 * <p>该接口只管理以具体 {@code short} 路径值索引的精确匹配分支；
 * 通配符分支由 {@link HandlerRegistry.TrieNode#wildcard()} 单独保存。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.1.0
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
