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

package com.nebilux.nexalithic.core.infra.loadbalance;

/**
 * 可负载均衡的
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface LoadBalanceable {
    /**
     * 获取当前组件的“负载分值”
     * 分值越低，代表越空闲，越应该被选中
     */
    long getLoadScore();
}