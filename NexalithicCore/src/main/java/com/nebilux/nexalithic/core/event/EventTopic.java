package com.nebilux.nexalithic.core.event;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 活动主题
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class EventTopic<T extends NexalithicEvent> {
    private final Queue<EventHandler<T>> subscribers = new ConcurrentLinkedQueue<>();
    private volatile boolean subscribed = false;

    public boolean isSubscribed() {
        return subscribed;
    }

    public EventSubscription subscribe(EventHandler<T> handler) {
        subscribed = true;
        if (handler.isPersistent() && !handler.isDynamic()) {
            subscribers.add(handler);
            return () -> this.unsubscribe(handler);
        } else {
            EventHandler<T> autoUnsubscribeWrapper = new EventHandler<>() {
                @Override
                public void handle(T event) {
                    try {
                        handler.handle(event);
                    } finally {
                        if (!handler.isPersistent()) {
                            unsubscribe(this);
                        }
                    }
                }

                @Override
                public boolean isPersistent() {
                    return true;
                }
            };
            subscribers.add(autoUnsubscribeWrapper);
            return () -> this.unsubscribe(autoUnsubscribeWrapper);
        }
    }

    public void unsubscribe(EventHandler<T> handler) {
        subscribers.remove(handler);
        subscribed = !subscribers.isEmpty();
    }

    public void publish(T event) {
        for (EventHandler<T> subscriber : subscribers) {
            subscriber.handle(event);
        }
    }
}
