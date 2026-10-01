package com.nebilux.nexalithic.server.manager;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.event.EventDefinition;
import com.nebilux.nexalithic.core.event.EventTopic;
import com.nebilux.nexalithic.core.event.NexalithicEvent;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.session.SessionAttachment;
import com.nebilux.nexalithic.core.session.SessionKey;
import com.nebilux.nexalithic.server.NexalithicServer;
import com.nebilux.nexalithic.server.session.ServerSession;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 会话管理器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SessionsManager {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, SessionsManager.class);
    public static final class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> Sessions_Initial_Capacity = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> Tokens_Initial_Capacity = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> Sessions_Lock_Stripes = defineOption(
                1024, OptionValidator.powerOfTwo()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    public static final class Events extends EventDefinition {
        public record NamedSession(String sessionName) implements NexalithicEvent {}
        public record RemovedSession(String sessionName, SessionAttachment attachment) implements NexalithicEvent {}
        private final EventTopic<NamedSession> NamedSessionTopic;
        private final EventTopic<RemovedSession> RemovedSessionTopic;
        private Events(EventTopic<NamedSession> namedSessionTopic, EventTopic<RemovedSession> removedSessionTopic) {
            NamedSessionTopic = namedSessionTopic;
            RemovedSessionTopic = removedSessionTopic;
        }
    }
    private static final ThreadLocal<SessionKey.Mutable> LOOKUP_KEY = ThreadLocal.withInitial(SessionKey.Mutable::new);
    private final Map<SessionKey, ServerSession> idToSessions;
    private final Map<String, ServerSession> nameToSessions;
    private final Map<SessionKey, ServerSession> tokens;
    private final ReentrantLock[] stripes;
    private final Events events;

    public SessionsManager(NexalithicBuilderContext context) {
        NexalithicEventBus eventBus = context.getModule(NexalithicServer.MODULES.EventBus);
        events = new Events(
                eventBus.registerTopic(Events.NamedSession.class),
                eventBus.registerTopic(Events.RemovedSession.class)
        );
        idToSessions = new ConcurrentHashMap<>(context.getOption(OPTIONS.Sessions_Initial_Capacity));
        nameToSessions = new ConcurrentHashMap<>(context.getOption(OPTIONS.Sessions_Initial_Capacity));
        tokens = new ConcurrentHashMap<>(context.getOption(OPTIONS.Tokens_Initial_Capacity));
        stripes = new ReentrantLock[context.getOption(OPTIONS.Sessions_Lock_Stripes)];
        for (int i = 0; i < stripes.length; i++) {
            stripes[i] = new ReentrantLock();
        }
    }

    public boolean putSession(ServerSession session) {
        return idToSessions.putIfAbsent(session.getSessionKey(), session) == null;
    }

    /**
     * 抢占式设置：返回被抢占的Session
     */
    public ServerSession forceSetSessionName(String name, ServerSession session) {
        ReentrantLock lock = getLock(name);
        lock.lock();
        try {
            ServerSession existing = nameToSessions.put(name, session);
            if (existing != null) {
                idToSessions.remove(existing.getSessionKey());
                if (events.RemovedSessionTopic.isSubscribed()) {
                    events.RemovedSessionTopic.publish(new Events.RemovedSession(name, existing.attachment()));
                }
            }
            session.setSessionName(name);
            if (events.NamedSessionTopic.isSubscribed()) {
                events.NamedSessionTopic.publish(new Events.NamedSession(name));
            }
            return existing;
        } finally {
            lock.unlock();
        }
    }
    /**
     * 保护式设置：如果名字已存在，返回 false 且不修改现有状态
     */
    public boolean trySetSessionName(String name, ServerSession session) {
        ReentrantLock lock = getLock(name);
        lock.lock();
        try {
            if (nameToSessions.putIfAbsent(name, session) == null) {
                session.setSessionName(name);
                if (events.NamedSessionTopic.isSubscribed()) {
                    events.NamedSessionTopic.publish(new Events.NamedSession(name));
                }
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public ServerSession getSession(SessionKey sessionKey) {
        return idToSessions.get(sessionKey);
    }
    public ServerSession getSession(String sessionName) {
        return nameToSessions.get(sessionName);
    }

    public void removeSession(ServerSession session) {
        if (session == null) {
            return;
        }
        idToSessions.remove(session.getSessionKey());
        String sessionName = session.getSessionName();
        if (sessionName != null) {
            ReentrantLock lock = getLock(sessionName);
            lock.lock();
            try {
                nameToSessions.remove(sessionName, session);
            } finally {
                lock.unlock();
            }
        }
        if (events.RemovedSessionTopic.isSubscribed()) {
            events.RemovedSessionTopic.publish(new Events.RemovedSession(sessionName, session.attachment()));
        }
    }
    public ServerSession removeSession(String sessionName) {
        ReentrantLock lock = getLock(sessionName);
        lock.lock();
        try {
            ServerSession session = nameToSessions.remove(sessionName);
            if (session == null) {
                return null;
            }
            idToSessions.remove(session.getSessionKey());
            if (events.RemovedSessionTopic.isSubscribed()) {
                events.RemovedSessionTopic.publish(new Events.RemovedSession(sessionName, session.attachment()));
            }
            return session;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 遍历所有已命名的在线会话
     * @param action 对每个会话执行的操作
     */
    public void forEachNamedSession(Consumer<ServerSession> action) {
        nameToSessions.values().forEach(action);
    }
    public void forEachSession(Consumer<ServerSession> action) {
        idToSessions.values().forEach(action);
    }

    public Collection<String> allSessionName() {
        return nameToSessions.keySet();
    }

    public void relateChannelToken(SessionKey.Immutable sessionKey, ServerSession session) {
        tokens.put(sessionKey, session);
    }
    public ServerSession verifyAndConsumeToken(ByteBuffer buffer, int offset) {
        return tokens.remove(LOOKUP_KEY.get().wrap(buffer, offset));
    }

    private ReentrantLock getLock(String name) {
        int h = name.hashCode();
        h ^= (h >>> 16);
        return stripes[h & (stripes.length - 1)];
    }
}
