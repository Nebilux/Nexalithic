package com.nebilux.nexalithic.core.messaging.task.visual;

/**
 * 监听函数
 *
 * @author Reonvia
 * @since 0.1.0
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
