package com.thezeroer.nexalithic.core.messaging.handler;

import com.thezeroer.nexalithic.core.messaging.handler.mapping.HandlerPathMatcher;

/**
 * Handler 元数据。
 *
 * <p>元数据用于日志、调试和拦截器回调。它不参与业务处理本身，
 * 但会随同 Handler 的拦截器管线一起传递，使拦截器能够识别当前处理目标。</p>
 *
 * @param name Handler 名称；未显式指定时通常由路径自动生成
 * @param description Handler 描述文本；未指定时为空字符串
 * @param pathMatcher 该 Handler 注册时使用的路径匹配器
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/09
 */
public record HandlerMetadata(String name, String description, HandlerPathMatcher pathMatcher) {

    /**
     * 返回适合日志输出的元数据描述。
     *
     * @return 包含名称、描述和路径匹配器的字符串
     */
    @Override
    public String toString() {
        return "HandlerMetadata [name = \"" + name + "\", description = \"" + description + "\", pathMatcher = " + pathMatcher + "\"]";
    }
}
