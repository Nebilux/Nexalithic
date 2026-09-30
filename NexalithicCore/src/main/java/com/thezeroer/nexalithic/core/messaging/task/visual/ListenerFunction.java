package com.thezeroer.nexalithic.core.messaging.task.visual;

/**
 * 监听函数
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/06
 * @version 1.0.0
 */
public interface ListenerFunction {
    @FunctionalInterface
    interface onStarted extends ListenerFunction {
        void execute(TransferSnapshot snapshot);
    }
    @FunctionalInterface
    interface onUpdated extends ListenerFunction {
        void execute(TransferSnapshot snapshot);
    }
    @FunctionalInterface
    interface onPaused extends ListenerFunction {
        void execute();
    }
    @FunctionalInterface
    interface onResume extends ListenerFunction {
        void execute();
    }
    @FunctionalInterface
    interface onFinished extends ListenerFunction {
        void execute();
    }
}
