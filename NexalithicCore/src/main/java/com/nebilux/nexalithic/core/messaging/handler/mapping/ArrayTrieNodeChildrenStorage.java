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

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * 基于数组的 Trie 精确子节点存储。
 *
 * <p>适用于路径值范围固定且连续的场景，例如协议 ID 固定在 {@code 0-255}。
 * 读取和写入都是数组下标访问；超出数组范围的 key 会被视为不存在。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ArrayTrieNodeChildrenStorage<HC extends HandlerContext<?>> implements TrieNodeChildrenStorage<HC> {
    private final HandlerRegistry.TrieNode<HC>[] array;

    /**
     * 创建指定容量的数组子节点存储。
     *
     * @param size 可直接索引的 key 数量
     */
    @SuppressWarnings("unchecked")
    public ArrayTrieNodeChildrenStorage(int size) {
        this.array = (HandlerRegistry.TrieNode<HC>[]) new HandlerRegistry.TrieNode[size];
    }

    @Override
    public HandlerRegistry.TrieNode<HC> get(short key) {
        return (key >= 0 && key < array.length) ? array[key] : null;
    }

    @Override
    public void put(short key, HandlerRegistry.TrieNode<HC> node) {
        if (key >= 0 && key < array.length) array[key] = node;
    }

    @Override
    public Collection<HandlerRegistry.TrieNode<HC>> all() {
        return Arrays.stream(array).filter(Objects::nonNull).toList();
    }
}
