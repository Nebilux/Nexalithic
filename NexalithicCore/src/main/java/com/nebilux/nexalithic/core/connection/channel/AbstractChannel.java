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

import com.nebilux.nexalithic.core.connection.transport.AbstractTransport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static com.nebilux.nexalithic.core.exception.FailureSupport.mergePreservingFirst;

/**
 * 抽象通道
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class AbstractChannel<C extends SelectableChannel> implements NexalithicChannel {
    /** 当前 Selector 注册与传输实例的绑定；Selector 重建时只替换 Key。 */
    public record Binding<C extends SelectableChannel>(AbstractTransport<C> transport, SelectionKey selectionKey) {
        private Binding<C> replaceSelectionKey(SelectionKey newSelectionKey) {
            return new Binding<>(transport, newSelectionKey);
        }
    }
    
    private final Kind kind;
    private final AtomicReference<State> state = new AtomicReference<>(State.Closed);
    private final Object lifecycleLock = new Object();
    private volatile Binding<C> binding;
    private volatile long lastActiveTimeNanos = -1;

    public AbstractChannel(Kind kind) {
        this.kind = kind;
    }

    public final boolean beginOpen() {
        return state.compareAndSet(State.Closed, State.Opening);
    }

    /**
     * 撤销尚未安装传输资源的打开过程。
     *
     * <p>该方法用于连接、握手等前置阶段失败后的状态回滚。它不会调用
     * {@link #onClose(Binding)}，因此不会把一次尚未完成的打开误认为已经建立的
     * Channel 被关闭。</p>
     *
     * @return 当前调用是否把状态从 {@link State#Opening} 回滚到 {@link State#Closed}
     */
    public final boolean abortOpen() {
        synchronized (lifecycleLock) {
            if (binding != null) {
                return false;
            }
            return state.compareAndSet(State.Opening, State.Closed);
        }
    }

    public final void open(SelectionKey selectionKey, AbstractTransport<C> transport) throws IOException {
        Objects.requireNonNull(selectionKey, "selectionKey");
        Objects.requireNonNull(transport, "transport");
        checkOnLifecycle();
        synchronized (lifecycleLock) {
            State current = state.get();
            if (current == State.Closed) {
                if (state.compareAndSet(State.Closed, State.Opening)) {
                    current = State.Opening;
                } else {
                    current = state.get();
                }
            }
            if (current != State.Opening) {
                throw new IllegalStateException("Cannot open channel while in state " + current);
            }
            if (!selectionKey.isValid()) {
                throw new IOException("SelectionKey is already invalid");
            }
            if (selectionKey.channel() != transport.selectableChannel()) {
                throw new IllegalArgumentException("SelectionKey belongs to another transport channel");
            }
            Binding<C> opening = new Binding<>(transport, selectionKey);
            binding = opening;
            lastActiveTimeNanos = System.nanoTime();
            try {
                selectionKey.attach(this);
                onOpen(opening);
                if (!state.compareAndSet(State.Opening, State.Opened)) {
                    throw new IllegalStateException(
                            "Cannot complete channel opening: expected Opening, actual " + state.get()
                    );
                }
            } catch (Throwable failure) {
                if (binding == opening) {
                    binding = null;
                }
                lastActiveTimeNanos = -1;
                Throwable terminalFailure = releaseBinding(opening, failure);
                state.compareAndSet(State.Opening, State.Closed);
                rethrow(terminalFailure);
            }
        }
    }

    public final void replaceSelectionKey(SelectionKey newSelectionKey) {
        Objects.requireNonNull(newSelectionKey, "newSelectionKey");
        checkOnLifecycle();
        synchronized (lifecycleLock) {
            if (state.get() != State.Opened) {
                throw new IllegalStateException("Cannot replace SelectionKey while in state " + state.get());
            }
            Binding<C> current = binding;
            if (current == null) {
                throw new IllegalStateException("Opened channel has no transport");
            }
            if (newSelectionKey.channel() != current.transport().selectableChannel()) {
                throw new IllegalArgumentException("Replacement SelectionKey belongs to another channel");
            }
            if (!newSelectionKey.isValid()) {
                throw new IllegalArgumentException("Replacement SelectionKey is invalid");
            }
            Binding<C> replacement = current.replaceSelectionKey(newSelectionKey);
            newSelectionKey.attach(this);
            binding = replacement;
            try {
                onReplaceSelectionKey(newSelectionKey);
            } catch (Throwable failure) {
                binding = current;
                newSelectionKey.cancel();
                if (failure instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                if (failure instanceof Error error) {
                    throw error;
                }
                throw new IllegalStateException("Failed to replace SelectionKey", failure);
            }
        }
    }

    public final AbstractTransport<C> getTransport() {
        Binding<C> current = binding;
        return current == null ? null : current.transport();
    }
    public final SelectionKey getSelectionKey() {
        Binding<C> current = binding;
        return current == null ? null : current.selectionKey();
    }
    public final C getSelectableChannel() {
        AbstractTransport<C> transport = getTransport();
        return transport == null ? null : transport.selectableChannel();
    }
    public final InetSocketAddress getRemoteAddress() {
        AbstractTransport<C> transport = getTransport();
        return transport == null ? null : transport.remoteAddress();
    }

    @Override
    public final Kind getKind() {
        return kind;
    }

    @Override
    public final State getState() {
        return state.get();
    }

    @Override
    public final long getLastActiveTimeNanos() {
        return lastActiveTimeNanos;
    }

    @Override
    public final boolean close() throws IOException {
        checkOnLifecycle();
        synchronized (lifecycleLock) {
            if (!beginClose()) {
                return false;
            }
            Binding<C> closing = binding;
            binding = null;
            lastActiveTimeNanos = -1;
            Throwable failure;
            try {
                failure = releaseBinding(closing, null);
            } finally {
                state.set(State.Closed);
            }
            if (failure != null) {
                rethrow(failure);
            }
            return true;
        }
    }

    protected final void updateLastActiveTimeNanos(long lastActiveTimeNanos) {
        this.lastActiveTimeNanos = lastActiveTimeNanos;
        onUpdateLastActiveTimeNanos(lastActiveTimeNanos);
    }

    protected void checkOnLifecycle() {}
    protected void onOpen(Binding<C> binding) {}
    protected void onReplaceSelectionKey(SelectionKey newSelectionKey) {}
    protected void onUpdateLastActiveTimeNanos(long lastActiveTimeNanos) {}
    protected void onClose(Binding<C> binding) {}

    private boolean beginClose() {
        while (true) {
            State current = state.get();
            if (current == State.Closed || current == State.Closing) {
                return false;
            }
            if (state.compareAndSet(current, State.Closing)) {
                return true;
            }
        }
    }
    private Throwable releaseBinding(Binding<C> closing, Throwable previousFailure) {
        Throwable failure = previousFailure;
        if (closing != null) {
            SelectionKey key = closing.selectionKey();
            if (key != null) {
                try {
                    key.cancel();
                } catch (Throwable throwable) {
                    failure = mergePreservingFirst(failure, throwable);
                }
            }
            try {
                closing.transport().close();
            } catch (Throwable throwable) {
                failure = mergePreservingFirst(failure, throwable);
            }
        }
        try {
            onClose(closing);
        } catch (Throwable throwable) {
            failure = mergePreservingFirst(failure, throwable);
        }
        return failure;
    }
    private static void rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException ioException) {
            throw ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IOException("Channel lifecycle operation failed", failure);
    }
}
