package com.thezeroer.nexalithic.core.io.channel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 抽象通道
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/19
 */
public abstract class AbstractChannel<C extends SelectableChannel> implements NexalithicChannel {
    public record Transport<C extends SelectableChannel> (C selectableChannel, SelectionKey selectionKey, InetSocketAddress remoteAddress) {
        private Transport<C> replaceSelectionKey(SelectionKey newSelectionKey) {
            return new Transport<>(selectableChannel, newSelectionKey, remoteAddress);
        }
    }
    protected final AtomicReference<State> state = new AtomicReference<>(State.Closed);
    protected volatile Transport<C> transport;
    protected volatile long lastActiveTimeNanos = -1;

    public final boolean beginOpen() {
        return state.compareAndSet(State.Closed, State.Opening);
    }

    public final void open(C selectableChannel, SelectionKey selectionKey, InetSocketAddress remoteAddress) throws IOException {
        checkOnLifecycle();
        transport = new Transport<>(selectableChannel, selectionKey, remoteAddress);
        selectionKey.attach(this);
        onOpen(transport);
    }

    public void replaceSelectionKey(SelectionKey newSelectionKey) {
        transport = transport.replaceSelectionKey(newSelectionKey);
        onReplaceSelectionKey(newSelectionKey);
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
    public boolean close() throws IOException {
        checkOnLifecycle();
        if (!beginClose()) {
            return false;
        }
        Transport<C> closing = this.transport;
        transport = null;
        lastActiveTimeNanos = -1;
        try {
            if (closing != null) {
                closing.selectableChannel.close();
            }
        } finally {
            try {
                onClose(closing);
            } finally {
                completeClose();
            }
        }
        return true;
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
    private void completeClose() {
        if (!state.compareAndSet(State.Closing, State.Closed)) {
            throw new IllegalStateException(
                    "Cannot complete channel closing: expected CLOSING, but was "
                            + state.get()
            );
        }
    }
}
