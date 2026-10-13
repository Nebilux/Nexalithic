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
import com.nebilux.nexalithic.core.infra.concurrent.atomic.AtomicAdmissionQueue;
import com.nebilux.nexalithic.core.session.SessionChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 会话循环
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class SessionLoop<H extends ChannelLoop.Handoff,C extends SessionChannel<?, ?>> extends ChannelLoop<H, C> {
    private static final Logger logger = LoggerFactory.getLogger(SessionLoop.class);
    private final AtomicAdmissionQueue<C> rateUpdateQueue;
    private final Set<C> queuedRateUpdates;

    public SessionLoop(NexalithicBuilderContext context, Options options) throws IOException {
        super(context, options);
        if (!supportsRateUpdates(context)) {
            queuedRateUpdates = null;
            rateUpdateQueue = null;
            return;
        }
        queuedRateUpdates = ConcurrentHashMap.newKeySet();
        rateUpdateQueue = new AtomicAdmissionQueue<>(new ConcurrentLinkedQueue<>(), queuedRateUpdates::add);
        int interestQueueDrainLimit = context.getOption(options.InterestQueue_DrainLimit);

        registerPreSelectTask(() -> {
            for (int remaining = interestQueueDrainLimit; remaining > 0; remaining--) {
                C channel = rateUpdateQueue.poll();
                if (channel == null) {
                    break;
                }
                queuedRateUpdates.remove(channel);
                channel.applyRate();
            }
        });
        registerImmediateWorkCondition(() -> !rateUpdateQueue.isEmpty());
        registerDrainCondition(rateUpdateQueue::isEmpty);
    }

    public void postRateUpdate(C channel) {
        if (rateUpdateQueue == null) {
            throw new UnsupportedOperationException("Rate updates are not supported by " + name);
        }
        if (inEventLoop()) {
            channel.applyRate();
            return;
        }
        if (rateUpdateQueue.offer(channel)) {
            wakeup();
        }
    }

    @Override
    protected void onOwnedChannelsCleared() {
        if (rateUpdateQueue != null) {
            rateUpdateQueue.close();
            rateUpdateQueue.drain(queuedRateUpdates::remove);
        }
    }

    @Override
    protected void onSelectionKeyMigrated(SelectionKey oldKey, SelectionKey newKey) {
        if (oldKey.attachment() instanceof SessionChannel<?, ?> channel) {
            channel.replaceSelectionKey(newKey);
        }
    }

    @Override
    protected boolean onExecuteReleaseEvent(C channel) {
        SelectionKey key = channel.getSelectionKey();
        if (key != null) {
            key.cancel();
        }
        return true;
    }

    @Override
    protected final void onChannelReady(SelectionKey selectionKey, C channel) {
        try {
            if (selectionKey.isReadable()) {
                if (channel.read() == -1) {
                    submitDisconnectEvent(channel, Event.Disconnect.Reason.REMOTE_CLOSED);
                    return;
                }
                dispatchReceivedPackets(channel);
            } else if (selectionKey.isWritable()) {
                channel.write();
            } else {
                submitDisconnectEvent(channel, Event.Disconnect.Reason.PROTOCOL_VIOLATION);
            }
        } catch (GeneralSecurityException e) {
            logger.warn("Channel[{}] onReadyEvent[{}] error", channel, name, e);
            submitDisconnectEvent(channel, Event.Disconnect.Reason.SECURITY_FAILURE);
        } catch (IOException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Channel[{}] onReadyEvent[{}] error", channel, name, e);
            }
            submitDisconnectEvent(channel, Event.Disconnect.Reason.IO_FAILURE);
        }
    }


    /** 子类可返回是否处理限速更新；该方法在 SessionLoop 构造期间调用，不应读取子类字段。 */
    protected abstract boolean supportsRateUpdates(NexalithicBuilderContext context);
    protected abstract void dispatchReceivedPackets(C channel);
}
