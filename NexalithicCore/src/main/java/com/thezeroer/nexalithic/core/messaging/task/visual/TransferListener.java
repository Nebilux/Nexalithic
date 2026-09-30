package com.thezeroer.nexalithic.core.messaging.task.visual;

/**
 * 传输监听器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/03
 * @version 1.0.0
 */
public class TransferListener {
    private final ListenerFunction.onStarted onStartedListener;
    private final ListenerFunction.onUpdated onUpdatedListener;
    private final ListenerFunction.onPaused onPausedListener;
    private final ListenerFunction.onResume onResumedListener;
    private final ListenerFunction.onFinished onFinishedListener;
    private volatile TransferSnapshot transferSnapshot;

    public TransferListener(ListenerFunction.onStarted onStartedListener, ListenerFunction.onUpdated onUpdatedListener,
                            ListenerFunction.onPaused onPausedListener, ListenerFunction.onResume onResumedListener, ListenerFunction.onFinished onFinishedListener) {
        this.onStartedListener = onStartedListener;
        this.onUpdatedListener = onUpdatedListener;
        this.onPausedListener = onPausedListener;
        this.onResumedListener = onResumedListener;
        this.onFinishedListener = onFinishedListener;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void onStarted(TransferSnapshot transferSnapshot) {
        this.transferSnapshot = transferSnapshot;
        onStartedListener.execute(transferSnapshot);
    }
    public void onUpdated() {
        onUpdatedListener.execute(transferSnapshot);
    }
    public void onPaused() {
        onPausedListener.execute();
    }
    public void onResumed() {
        onResumedListener.execute();
    }
    public void onFinished() {
        onFinishedListener.execute();
    }

    public static class Builder {
        private ListenerFunction.onStarted onStartedListener = snapshot -> {};
        private ListenerFunction.onUpdated onUpdatedListener = snapshot -> {};
        private ListenerFunction.onPaused onPausedListener = () -> {};
        private ListenerFunction.onResume onResumedListener = () -> {};
        private ListenerFunction.onFinished onFinishedListener = () -> {};

        public Builder onStarted(ListenerFunction.onStarted onStartedListener) {
            this.onStartedListener = onStartedListener;
            return this;
        }
        public Builder onUpdated(ListenerFunction.onUpdated onUpdatedListener) {
            this.onUpdatedListener = onUpdatedListener;
            return this;
        }
        public Builder onPaused(ListenerFunction.onPaused onPausedListener) {
            this.onPausedListener = onPausedListener;
            return this;
        }
        public Builder onResumed(ListenerFunction.onResume onResumedListener) {
            this.onResumedListener = onResumedListener;
            return this;
        }
        public Builder onFinished(ListenerFunction.onFinished onFinishedListener) {
            this.onFinishedListener = onFinishedListener;
            return this;
        }

        public TransferListener build() {
            return new TransferListener(onStartedListener, onUpdatedListener, onPausedListener, onResumedListener, onFinishedListener);
        }
    }
}
