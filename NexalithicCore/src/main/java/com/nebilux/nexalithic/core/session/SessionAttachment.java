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

package com.nebilux.nexalithic.core.session;

/**
 * <h3>Nexalithic 会话附件规范接口</h3>
 *
 * <p>该接口用于定义附加在 {@code NexalithicSession} 上的自定义业务对象。
 * 强制实现此接口是为了确保业务数据与底层会话的生命周期能够精确同步。</p>
 *
 * <p><b>设计初衷：</b></p>
 * <ul>
 * <li><b>显式清理：</b> 避免开发者直接将原始对象（如 {@code List} 或 {@code Map}）丢入 Session 导致的隐性内存泄漏。</li>
 * <li><b>托管回收：</b> 当网络异常断开、心跳超时或主动关闭时，Nexalithic 会自动回调 {@link #clear()} 方法。</li>
 * </ul>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface SessionAttachment {

    /**
     * 当关联的 {@code NexalithicSession} 被销毁或移除附件时，此方法会被自动触发。
     * <p>开发者应在此方法中执行：</p>
     * 1. 释放非堆内存（如 DirectBuffer）<br>
     * 2. 清空集合对象，断开与业务单例的强引用<br>
     * 3. 停止该会话专属的定时任务或后续回调
     */
    void clear();
}
