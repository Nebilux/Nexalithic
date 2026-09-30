package com.thezeroer.nexalithic.core.messaging.handler.mapping;

import com.thezeroer.nexalithic.core.exception.NexalithicDuplicateKeyException;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.NexalithicHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

/**
 * Handler 注册表。
 *
 * <p>注册表把 {@link HandlerPathMatcher} 描述的路径规则冻结为 Trie 树，
 * 并在运行时根据业务包路径匹配对应的 {@link NexalithicHandler}。</p>
 *
 * <p>每个 Trie 层级同时支持精确匹配分支和一个通配符分支。匹配时优先尝试精确分支；
 * 精确分支无法完整匹配时，再尝试同层级的通配符分支。</p>
 *
 * @param <HC> Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/16
 * @version 1.0.0
 */
public class HandlerRegistry<HC extends HandlerContext<?>> {
    private static final Logger logger = LoggerFactory.getLogger(HandlerRegistry.class);
    private final TrieNode<HC> root;
    /**
     * 使用冻结后的 Trie 根节点创建注册表。
     *
     * @param root Trie 根节点
     */
    private HandlerRegistry(TrieNode<HC> root) {
        this.root = root;
    }

    /**
     * 创建 Handler 注册表构建器。
     *
     * @param <HC> Handler 上下文类型
     * @return 注册表构建器
     */
    public static <HC extends HandlerContext<?>> Builder<HC> builder() {
        return new Builder<>();
    }

    /**
     * 根据路径匹配对应的处理器。
     *
     * <p>匹配规则：</p>
     * <ol>
     *     <li>路径长度必须完全一致；</li>
     *     <li>优先匹配精确节点；</li>
     *     <li>精确分支无法完整匹配时，再尝试通配符分支；</li>
     *     <li>不允许回退到较短路径上的Handler。</li>
     * </ol>
     *
     * @param path 协议路径数组
     * @return 匹配到的处理器，未匹配则返回null
     */
    public NexalithicHandler<HC> match(short[] path) {
        if (root == null) {
            return null;
        }
        if (path == null) {
            return root.handler();
        }
        NexalithicHandler<HC> handler = doMatch(root, path, 0);
        if (handler == null) {
            return root.handler();
        }
        return handler;
    }
    /**
     * 递归匹配。
     *
     * <p>通配符节点只匹配当前的一个路径层级。</p>
     *
     * @param current 当前 Trie 节点
     * @param path 待匹配路径
     * @param depth 当前匹配深度
     * @return 匹配到的 Handler；未匹配时返回 {@code null}
     */
    private NexalithicHandler<HC> doMatch(TrieNode<HC> current, short[] path, int depth) {
        if (depth == path.length) {
            return current.handler();
        }
        TrieNode<HC> exactNode = current.children().get(path[depth]);
        if (exactNode != null) {
            NexalithicHandler<HC> exactHandler = doMatch(exactNode, path, depth + 1);
            if (exactHandler != null) {
                return exactHandler;
            }
        }
        TrieNode<HC> wildcardNode = current.wildcard();
        if (wildcardNode != null) {
            return doMatch(wildcardNode, path, depth + 1);
        }
        return null;
    }
    /**
     * {@link HandlerRegistry} 构建器。
     *
     * <p>构建阶段使用可变 Trie 节点收集所有路径绑定，
     * 调用 {@link #build()} 后会冻结为不可变的 {@link TrieNode} 结构。</p>
     *
     * @param <HC> Handler 上下文类型
     */
    public static class Builder<HC extends HandlerContext<?>> {
        private Function<Integer, TrieNodeChildrenStorage<HC>> factory = (count) -> new MapTrieNodeChildrenStorage<>();
        private final MutableNode<HC> root = new MutableNode<>();

        /**
         * 注册路径匹配器关联的 Handler。
         *
         * <p>一个匹配器可能包含多候选值层级，因此一次注册可能会在 Trie 中绑定多条实际路径。
         * 如果目标路径已经绑定了 Handler，则抛出重复键异常。</p>
         *
         * @param handler 业务逻辑处理器
         * @throws NexalithicDuplicateKeyException 目标路径已存在 Handler 时抛出
         */
        public Builder<HC> handler(NexalithicHandler<HC> handler) {
            HandlerPathMatcher matcher = handler.getMetadata().pathMatcher();
            if (matcher == null || handler == null) {
                return this;
            }
            List<short[]> levels = matcher.getLevels();
            if (levels.isEmpty()) {
                bindHandler(root, handler);
                return this;
            }
            doRegister(root, levels, 0, handler);
            return this;
        }

        public Builder<HC> handlers(Collection<NexalithicHandler<HC>> handlers) {
            for (NexalithicHandler<HC> handler : handlers) {
                handler(handler);
            }
            return this;
        }

        /**
         * 设置冻结 Trie 时使用的精确子节点存储工厂。
         *
         * <p>函数入参是当前节点精确子节点数量，返回值决定该节点使用数组存储还是映射存储。</p>
         *
         * @param factory 子节点存储工厂
         */
        public Builder<HC> trieNodeChildrenStorageFactory(Function<Integer, TrieNodeChildrenStorage<HC>> factory) {
            this.factory = factory;
            return this;
        }

        /**
         * 递归注册指定深度的路径层级。
         */
        private void doRegister(MutableNode<HC> parent, List<short[]> levels, int depth, NexalithicHandler<HC> handler) {
            short[] level = levels.get(depth);
            if (level.length == 0) {
                registerNext(parent.getOrCreateWildcard(), levels, depth, handler);
            } else {
                for (short key : level) {
                    registerNext(parent.getOrCreateChild(key), levels, depth, handler);
                }
            }
        }
        /**
         * 注册当前层级后的下一步。
         */
        private void registerNext(MutableNode<HC> child, List<short[]> levels, int depth, NexalithicHandler<HC> handler) {
            if (depth == levels.size() - 1) {
                bindHandler(child, handler);
                return;
            }
            doRegister(child, levels, depth + 1, handler);
        }
        /**
         * 将 Handler 绑定到当前 Trie 节点。
         */
        private void bindHandler(MutableNode<HC> node, NexalithicHandler<HC> handler) {
            NexalithicHandler<HC> existing = node.handler;
            if (existing != null) {
                throw new NexalithicDuplicateKeyException("HandlerRegistry", existing.getMetadata());
            }
            node.handler = handler;
            logger.debug("Registered handler mappings, {}", handler.getMetadata());
        }

        /**
         * 构建不可变 Handler 注册表。
         *
         * @return Handler 注册表
         */
        public HandlerRegistry<HC> build() {
            return new HandlerRegistry<>(freeze(root));
        }

        /**
         * 将可变节点递归冻结为不可变 Trie 节点。
         */
        private TrieNode<HC> freeze(MutableNode<HC> mutable) {
            TrieNodeChildrenStorage<HC> storage = factory.apply(mutable.children.size());
            for (Map.Entry<Short, MutableNode<HC>> entry : mutable.children.entrySet()) {
                storage.put(entry.getKey(), freeze(entry.getValue()));
            }
            TrieNode<HC> wildcard = (mutable.wildcard != null) ? freeze(mutable.wildcard) : null;
            return new TrieNode<>(storage, wildcard, mutable.handler);
        }

        /**
         * 构建阶段使用的可变 Trie 节点。
         *
         * @param <HC> Handler 上下文类型
         */
        private static class MutableNode<HC extends HandlerContext<?>> {
            final Map<Short, MutableNode<HC>> children = new HashMap<>();
            MutableNode<HC> wildcard;
            NexalithicHandler<HC> handler;

            /**
             * 获取或创建精确匹配子节点。
             */
            MutableNode<HC> getOrCreateChild(short key) {
                return children.computeIfAbsent(key, k -> new MutableNode<>());
            }

            /**
             * 获取或创建通配符子节点。
             */
            MutableNode<HC> getOrCreateWildcard() {
                if (wildcard == null) {
                    wildcard = new MutableNode<>();
                }
                return wildcard;
            }
        }
    }

    /**
     * Trie 树节点定义
     * 优化方案：显式区分精确匹配分支与通配符分支
     *
     * @param <HC> Handler 上下文类型
     * @param children 精确匹配子节点：Key 为具体的协议 ID
     * @param wildcard 通配符匹配子节点：如果当前层级匹配 *，则流向此节点
     * @param handler  绑定的处理器实例
     */
    public record TrieNode<HC extends HandlerContext<?>>(TrieNodeChildrenStorage<HC> children,
                                                         TrieNode<HC> wildcard,
                                                         NexalithicHandler<HC> handler) {
    }
}
