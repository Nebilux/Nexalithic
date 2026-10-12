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

package com.nebilux.nexalithic.core.messaging.handler;

import com.nebilux.nexalithic.core.messaging.handler.mapping.HandlerPathMatcher;

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
 * @author Reonvia
 * @since 0.2.0
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
