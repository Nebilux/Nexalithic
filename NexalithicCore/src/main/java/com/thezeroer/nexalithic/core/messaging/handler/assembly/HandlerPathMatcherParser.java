package com.thezeroer.nexalithic.core.messaging.handler.assembly;

import com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation.HandlerPathLevel;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation.NexalithicHandlerController;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation.NexalithicHandlerMethod;
import com.thezeroer.nexalithic.core.messaging.handler.mapping.HandlerPathMatcher;

/**
 * Handler 路径匹配器解析工具。
 *
 * <p>负责将 {@link NexalithicHandlerController} 上的公共路径和
 * {@link NexalithicHandlerMethod} 上的方法路径合并为最终的
 * {@link HandlerPathMatcher}。合并顺序固定为 Controller 路径在前，
 * Handler 方法路径在后。</p>
 *
 * <p>每个注解只能使用 {@code value} 或 {@code levels} 其中一种路径声明方式。
 * 同时声明两者会被视为配置错误。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/24
 */
public final class HandlerPathMatcherParser {
    private HandlerPathMatcherParser() {}

    /**
     * 解析Controller与Handler方法的完整路径。
     *
     * <p>Controller路径在前，Handler方法路径在后。</p>
     *
     * @param controller Controller注解
     * @param method Handler方法注解
     * @return 完整路径匹配器
     * @throws IllegalArgumentException 任一注解同时声明 {@code value} 和 {@code levels} 时抛出
     */
    public static HandlerPathMatcher parse(NexalithicHandlerController controller, NexalithicHandlerMethod method) {
        HandlerPathMatcher matcher = new HandlerPathMatcher();
        append(matcher, controller.value(), controller.levels());
        append(matcher, method.value(), method.levels());
        return matcher;
    }

    /**
     * 将单个注解中的路径配置追加到匹配器。
     *
     * <p>{@code value} 会被解析为连续的精确匹配层级；
     * {@code levels} 中每个 {@link HandlerPathLevel} 会被解析为一个候选值层级或通配符层级。</p>
     *
     * @param matcher 输出路径匹配器
     * @param value 快捷路径声明
     * @param levels 完整路径层级声明
     */
    private static void append(HandlerPathMatcher matcher, short[] value, HandlerPathLevel[] levels) {
        if (value.length > 0 && levels.length > 0) {
            throw new IllegalArgumentException("value and levels cannot both be specified");
        }
        if (levels.length > 0) {
            for (HandlerPathLevel level : levels) {
                short[] options = level.value();
                if (options.length == 0) {
                    matcher.addWildcard();
                } else {
                    matcher.addBreadth(options);
                }
            }
            return;
        }
        if (value.length > 0) {
            matcher.addDepth(value);
        }
    }
}
