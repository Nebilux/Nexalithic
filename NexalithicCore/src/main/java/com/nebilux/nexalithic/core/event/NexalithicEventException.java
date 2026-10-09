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

package com.nebilux.nexalithic.core.event;

import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 事件总线中找不到指定事件主题的异常。
 *
 * <p>订阅或取消订阅某个事件类型时，如果该类型尚未注册主题，就会产生；
 * 事件类型的全限定名包含在异常消息中。</p>
 */
public final class NexalithicEventException extends NexalithicUncheckedException {
    private NexalithicEventException(String message) {
        super(message);
    }

    public static NexalithicEventException missingTopic(Class<? extends NexalithicEvent> eventType) {
        return new NexalithicEventException("No such topic for event " + eventType.getName());
    }
}
