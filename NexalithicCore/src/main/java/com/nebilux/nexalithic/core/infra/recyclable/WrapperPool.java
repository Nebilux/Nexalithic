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

package com.nebilux.nexalithic.core.infra.recyclable;

/**
 * 包装器池 (Wrapper Pool)
 * <p>负责管理 {@link RecyclableWrapper} 实例的生命周期。</p>
 *
 * @param <W> 池所管理的具体包装器类型，必须是自引用泛型。
 * @author Reonvia
 * @since 0.1.0
 */
public interface WrapperPool<W extends RecyclableWrapper<?>> {
    /**
     * 从池中获取一个可用的包装器。
     * <p>如果池为空，实现类应根据策略创建新实例或阻塞等待。</p>
     * @return 准备就绪的包装器实例
     */
    W acquire();

    /**
     * 执行池预热
     * @param prefillRatio 预热比例 (0.0 ~ 1.0)。
     * 例如 0.5 表示启动时填充 50% 的容量。
     * @throws IllegalArgumentException 如果比例不在有效范围内
     */
    WrapperPool<W> warmUp(double prefillRatio);
}
