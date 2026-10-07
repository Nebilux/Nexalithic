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

package com.nebilux.nexalithic.core.io.loop;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.session.SessionChannel;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 会话循环
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class SessionLoop<H extends ChannelLoop.Handoff,C extends SessionChannel<?, ?>> extends ChannelLoop<H, C> {
    public SessionLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
    }

    @Override
    protected void onSelectionKeyMigrated(SelectionKey oldKey, SelectionKey newKey) {
        if (oldKey.attachment() instanceof SessionChannel<?, ?> channel) {
            channel.replaceSelectionKey(newKey);
        }
    }

    public void postRateUpdate(C channel) {}
}
