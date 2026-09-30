package com.thezeroer.nexalithic.core.messaging.handler.mapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Handler 路径匹配器描述对象。
 *
 * <p>每个short数组表示一个路径层级：</p>
 * <ul>
 *     <li>非空数组表示当前层级允许匹配的候选值；</li>
 *     <li>空数组表示当前层级为通配符。</li>
 * </ul>
 *
 * <p>该类型只描述匹配规则本身，不执行匹配。实际匹配由
 * {@link HandlerRegistry} 在冻结后的 Trie 树上完成。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/16
 */
@SuppressWarnings("UnusedReturnValue")
public final class HandlerPathMatcher {

    /**
     * 路径层级。
     *
     * <p>外层列表为空表示根路径，内层空数组表示通配符层级。</p>
     */
    private final List<short[]> levels = new ArrayList<>();

    /**
     * 依次追加多个固定值层级。
     *
     * <p>例如{@code addDepth(1, 2)}表示路径{@code /1/2}。</p>
     *
     * @param paths 固定路径序列
     * @return 当前匹配器
     */
    public HandlerPathMatcher addDepth(short... paths) {
        if (paths == null) {
            return this;
        }
        for (short path : paths) {
            levels.add(new short[]{path});
        }
        return this;
    }

    /**
     * 追加一个包含多个候选值的路径层级。
     *
     * <p>例如{@code addBreadth(1, 2)}表示当前层级可以匹配
     * {@code 1}或{@code 2}。</p>
     *
     * @param options 当前层级的候选值
     * @return 当前匹配器
     */
    public HandlerPathMatcher addBreadth(short... options) {
        if (options == null || options.length == 0) {
            return this;
        }
        short[] unique = distinct(options);
        if (unique.length > 0) {
            levels.add(unique);
        }
        return this;
    }

    /**
     * 追加一个通配符层级。
     *
     * @return 当前匹配器
     */
    public HandlerPathMatcher addWildcard() {
        levels.add(new short[0]);
        return this;
    }

    /**
     * 返回路径层级的只读深拷贝。
     *
     * @return 路径层级
     */
    public List<short[]> getLevels() {
        List<short[]> result =
                new ArrayList<>(levels.size());

        for (short[] level : levels) {
            result.add(level.clone());
        }

        return Collections.unmodifiableList(result);
    }

    /**
     * 返回路径深度。
     *
     * @return 路径层级数量
     */
    public int depth() {
        return levels.size();
    }

    /**
     * 判断是否为根路径。
     *
     * @return 没有任何路径层级时返回 {@code true}
     */
    public boolean isEmpty() {
        return levels.isEmpty();
    }

    /**
     * 格式化路径。
     *
     * @return 用于日志和调试的路径字符串
     */
    public String formatPath() {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < levels.size(); i++) {
            if (i > 0) {
                builder.append('-');
            }
            short[] level = levels.get(i);
            if (isWildcardLevel(level)) {
                builder.append('*');
                continue;
            }
            builder.append('(');
            for (int j = 0; j < level.length; j++) {
                if (j > 0) {
                    builder.append('|');
                }
                builder.append(level[j]);
            }
            builder.append(')');
        }
        return builder.append(']').toString();
    }

    private static boolean isWildcardLevel(short[] level) {
        return level.length == 0;
    }

    private static short[] distinct(short[] values) {
        short[] result = new short[values.length];
        int size = 0;
        outer:
        for (short value : values) {
            for (int i = 0; i < size; i++) {
                if (result[i] == value) {
                    continue outer;
                }
            }
            result[size++] = value;
        }
        return Arrays.copyOf(result, size);
    }

    @Override
    public String toString() {
        return formatPath();
    }
}
