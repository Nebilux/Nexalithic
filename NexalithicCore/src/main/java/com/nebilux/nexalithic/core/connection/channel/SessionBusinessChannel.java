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

package com.nebilux.nexalithic.core.connection.channel;

import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.infra.rate.DynamicRateController;
import com.nebilux.nexalithic.core.infra.rate.RateLimiter;
import com.nebilux.nexalithic.core.io.codec.assembler.ModelAssembler;
import com.nebilux.nexalithic.core.io.codec.fragmenter.ModelFragmenter;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.security.SecretKeyContext;

import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.LongAdder;

/**
 * 会议业务频道
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class SessionBusinessChannel<M extends BusinessModel<?>, S extends AbstractSession<S>> extends SessionChannel<M, S> {
    private final RateLimiter rateLimiter = new RateLimiter(1024 * 1024);
    private final DynamicRateController.RateState rateState = new DynamicRateController.RateState();
    private final LongAdder readBytesWindow = new LongAdder();
    private final LongAdder writeBytesWindow = new LongAdder();

    public SessionBusinessChannel(Kind kind, SessionLoop<?, ? super SessionChannel<M, S>> ownerLoop, S ownerSession,
                                  ModelFragmenter<M> fragmenter, ModelAssembler<M> assembler, SecretKeyContext secretKeyContext) {
        super(kind, ownerLoop, ownerSession, fragmenter, assembler, secretKeyContext);
    }

    public void updateReadRate(long rate) {
        rateLimiter.updateReadRate(rate);
        ownerLoop.postRateUpdate(this);
    }

    public void updateWriteRate(long rate) {
        rateLimiter.updateWriteRate(rate);
        ownerLoop.postRateUpdate(this);
    }

    public final void applyRate() {
        rateLimiter.applyRate();
    }

    public final long evaluateDynamicRate(long intervalNanos, long nowNanos, DynamicRateController controller) {
        return controller.evaluateAndGetRate(readBytesWindow.sumThenReset(), intervalNanos, nowNanos, rateState);
    }

    public final void resetDynamicRateState() {
        rateState.reset();
        readBytesWindow.sumThenReset();
        writeBytesWindow.sumThenReset();
    }

    @Override
    protected long availableWriteBytes() {
        rateLimiter.refillWriteCredit();
        return rateLimiter.getWriteCredit();
    }

    @Override
    protected long availableReadBytes() {
        rateLimiter.refillReadCredit();
        return rateLimiter.getReadCredit();
    }

    @Override
    protected void onBytesWritten(long bytes) {
        rateLimiter.consumeWrite(bytes);
        if (bytes > 0) {
            writeBytesWindow.add(bytes);
        }
    }

    @Override
    protected void onBytesRead(long bytes) {
        rateLimiter.consumeRead(bytes);
        if (bytes > 0) {
            readBytesWindow.add(bytes);
        }
    }

    @Override
    protected void onClose(Binding<SocketChannel> binding) {
        super.onClose(binding);
        resetDynamicRateState();
    }
}
