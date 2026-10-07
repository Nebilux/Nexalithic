package com.nebilux.nexalithic.server.session;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.event.EventDefinition;
import com.nebilux.nexalithic.core.event.EventTopic;
import com.nebilux.nexalithic.core.event.NexalithicEvent;
import com.nebilux.nexalithic.core.event.NexalithicEventBus;
import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;
import com.nebilux.nexalithic.core.session.SessionAttachment;
import com.nebilux.nexalithic.core.session.SessionKey;
import com.nebilux.nexalithic.server.NexalithicServer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 会话管理器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SessionRegistry {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, SessionRegistry.class);
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
    private record ChannelGrant(ServerSession session, NexalithicChannel.Kind kind) {}

    private final Map<SessionKey, ChannelGrant> tokens;
    private final ReentrantLock[] stripes;
    private final Events events;

    public SessionRegistry(NexalithicBuilderContext context) {
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
                removeChannelTokens(existing);
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
        removeChannelTokens(session);
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
            removeChannelTokens(session);
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

    /**
     * 登记一次性通道令牌，并把令牌绑定到目标 Session 与通道类型。
     *
     * @return 令牌是否成功登记；随机碰撞时返回 {@code false}
     */
    public boolean relateChannelToken(
            SessionKey.Immutable sessionKey,
            ServerSession session,
            NexalithicChannel.Kind kind
    ) {
        return tokens.putIfAbsent(
                Objects.requireNonNull(sessionKey, "sessionKey"),
                new ChannelGrant(
                        Objects.requireNonNull(session, "session"),
                        Objects.requireNonNull(kind, "kind")
                )
        ) == null;
    }

    /** 验证并消费一次性令牌；通道类型不匹配时同样消费并拒绝。 */
    public ServerSession verifyAndConsumeToken(
            ByteBuffer buffer,
            int offset,
            NexalithicChannel.Kind kind
    ) {
        ChannelGrant grant = tokens.remove(LOOKUP_KEY.get().wrap(buffer, offset));
        return grant != null && grant.kind() == kind ? grant.session() : null;
    }

    /** 仅在信令响应未能入队时撤销刚登记的令牌。 */
    public boolean removeChannelToken(SessionKey sessionKey, ServerSession session) {
        ChannelGrant grant = tokens.get(sessionKey);
        return grant != null && grant.session() == session && tokens.remove(sessionKey, grant);
    }

    private void removeChannelTokens(ServerSession session) {
        tokens.forEach((token, grant) -> {
            if (grant.session() == session) {
                tokens.remove(token, grant);
            }
        });
    }

    private ReentrantLock getLock(String name) {
        int h = name.hashCode();
        h ^= (h >>> 16);
        return stripes[h & (stripes.length - 1)];
    }
}
