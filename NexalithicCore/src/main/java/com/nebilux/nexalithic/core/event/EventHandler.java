package com.nebilux.nexalithic.core.event;

/**
 * 事件处理器接口
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface EventHandler<T extends NexalithicEvent> {

    /**
     * 具体的业务逻辑
     * @param event 事件实例
     */
    void handle(T event);

    /**
     * 是否持久订阅
     * @return 返回 true 表示继续订阅，返回 false 表示执行后立即注销
     */
    boolean isPersistent();

    /**
     * 标记该处理器是否具有动态生命周期（状态是否会改变）
     * 默认 false，ConditionalHandler 及其子类需覆盖为 true
     */
    default boolean isDynamic() {
        return false;
    }

    /**
     * 永久订阅者：默认行为，始终存在
     */
    abstract class PersistentHandler<T extends NexalithicEvent> implements EventHandler<T> {
        @Override
        public final boolean isPersistent() {
            return true;
        }
    }

    /**
     * 一次性订阅者：执行一次后自动注销
     */
    abstract class OnceHandler<T extends NexalithicEvent> implements EventHandler<T> {
        @Override
        public final boolean isPersistent() {
            return false;
        }
    }

    /**
     * 条件订阅者：根据事件内容动态决定是否注销
     */
    abstract class ConditionalHandler<T extends NexalithicEvent> implements EventHandler<T> {
        private boolean shouldContinue = true;

        @Override
        public final void handle(T event) {
            shouldContinue = handleTrigger(event);
        }

        /**
         * @return 返回 true 表示下次还要，返回 false 表示这是最后一次
         */
        protected abstract boolean handleTrigger(T event);

        @Override
        public final boolean isPersistent() {
            return shouldContinue;
        }

        @Override
        public final boolean isDynamic() {
            return true;
        }
    }
}