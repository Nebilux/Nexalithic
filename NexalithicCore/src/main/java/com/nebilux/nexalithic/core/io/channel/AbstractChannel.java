package com.nebilux.nexalithic.core.io.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 抽象通道
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class AbstractChannel<C extends SelectableChannel> implements NexalithicChannel {
    public record Transport<C extends SelectableChannel> (C selectableChannel, SelectionKey selectionKey, InetSocketAddress remoteAddress) {
        private Transport<C> replaceSelectionKey(SelectionKey newSelectionKey) {
            return new Transport<>(selectableChannel, newSelectionKey, remoteAddress);
        }
    }
    private final Kind kind;
    private final AtomicReference<State> state = new AtomicReference<>(State.Closed);
    private final Object lifecycleLock = new Object();
    private volatile Transport<C> transport;
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
     * {@link #onClose(Transport)}，因此不会把一次尚未完成的打开误认为已经建立的
     * Channel 被关闭。</p>
     *
     * @return 当前调用是否把状态从 {@link State#Opening} 回滚到 {@link State#Closed}
     */
    public final boolean abortOpen() {
        synchronized (lifecycleLock) {
            if (transport != null) {
                return false;
            }
            return state.compareAndSet(State.Opening, State.Closed);
        }
    }

    public final void open(SelectionKey selectionKey, InetSocketAddress remoteAddress) throws IOException {
        Objects.requireNonNull(selectionKey, "selectionKey");
        Objects.requireNonNull(remoteAddress, "remoteAddress");
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
            @SuppressWarnings("unchecked")
            Transport<C> opening = new Transport<>((C) selectionKey.channel(), selectionKey, remoteAddress);
            transport = opening;
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
                if (transport == opening) {
                    transport = null;
                }
                lastActiveTimeNanos = -1;
                Throwable terminalFailure = releaseTransport(opening, failure);
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
            Transport<C> current = transport;
            if (current == null) {
                throw new IllegalStateException("Opened channel has no transport");
            }
            if (newSelectionKey.channel() != current.selectableChannel()) {
                throw new IllegalArgumentException("Replacement SelectionKey belongs to another channel");
            }
            if (!newSelectionKey.isValid()) {
                throw new IllegalArgumentException("Replacement SelectionKey is invalid");
            }
            Transport<C> replacement = current.replaceSelectionKey(newSelectionKey);
            newSelectionKey.attach(this);
            transport = replacement;
            try {
                onReplaceSelectionKey(newSelectionKey);
            } catch (Throwable failure) {
                transport = current;
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

    public final Transport<C> getTransport() {
        return transport;
    }
    public final C getSelectableChannel() {
        Transport<C> transport = this.transport;
        if (transport == null) {
            return null;
        }
        return transport.selectableChannel;
    }
    public final SelectionKey getSelectionKey() {
        Transport<C> transport = this.transport;
        if (transport == null) {
            return null;
        }
        return transport.selectionKey;
    }
    public final InetSocketAddress getRemoteAddress() {
        Transport<C> transport = this.transport;
        if (transport == null) {
            return null;
        }
        return transport.remoteAddress;
    }

    public final Kind getKind() {
        return kind;
    }

    @Override
    public void updateLastActiveTimeNanos(long lastActiveTimeNanos) {
        this.lastActiveTimeNanos = lastActiveTimeNanos;
        onUpdateLastActiveTimeNanos(lastActiveTimeNanos);
    }

    @Override
    public State getState() {
        return state.get();
    }

    @Override
    public long getLastActiveTimeNanos() {
        return lastActiveTimeNanos;
    }

    @Override
    public final boolean close() throws IOException {
        checkOnLifecycle();
        synchronized (lifecycleLock) {
            if (!beginClose()) {
                return false;
            }
            Transport<C> closing = transport;
            transport = null;
            lastActiveTimeNanos = -1;
            Throwable failure;
            try {
                failure = releaseTransport(closing, null);
            } finally {
                state.set(State.Closed);
            }
            if (failure != null) {
                rethrow(failure);
            }
            return true;
        }
    }

    protected void checkOnLifecycle() {}
    protected void onOpen(Transport<C> transport) {}
    protected void onReplaceSelectionKey(SelectionKey newSelectionKey) {}
    protected void onUpdateLastActiveTimeNanos(long lastActiveTimeNanos) {}
    protected void onClose(Transport<C> transport) {}

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
    private Throwable releaseTransport(Transport<C> closing, Throwable previousFailure) {
        Throwable failure = previousFailure;
        if (closing != null) {
            SelectionKey key = closing.selectionKey();
            if (key != null) {
                try {
                    key.cancel();
                } catch (Throwable throwable) {
                    failure = mergeThrowable(failure, throwable);
                }
            }
            try {
                closing.selectableChannel().close();
            } catch (Throwable throwable) {
                failure = mergeThrowable(failure, throwable);
            }
        }
        try {
            onClose(closing);
        } catch (Throwable throwable) {
            failure = mergeThrowable(failure, throwable);
        }
        return failure;
    }
    private static Throwable mergeThrowable(Throwable previous, Throwable current) {
        if (previous == null) {
            return current;
        }
        if (current != null && current != previous) {
            previous.addSuppressed(current);
        }
        return previous;
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
