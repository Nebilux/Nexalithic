package com.thezeroer.nexalithic.core.builder;

import com.thezeroer.nexalithic.core.builder.module.NexalithicModule;
import com.thezeroer.nexalithic.core.builder.option.NexalithicOption;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Nexalithic 构建上下文
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/03
 * @version 1.0.0
 */
public class NexalithicBuilderContext {
    private final Map<NexalithicOption<?>, Object> options = new ConcurrentHashMap<>();
    private final Map<NexalithicModule<?>, Object> modules = new ConcurrentHashMap<>();
    private final Map<CompositeKey, Object> constants = new ConcurrentHashMap<>();
    /**
     * 当前构建上下文内的实例编号器。
     * <p>
     * namespace 用于区分不同类别的编号，
     * type 用于决定是否按具体类型分别编号。
     */
    private final ConcurrentMap<CompositeKey, AtomicInteger> ordinals = new ConcurrentHashMap<>();

    public NexalithicBuilderContext() {}

    public <T> void setOption(NexalithicOption<T> option, T value) {
        options.put(option.validate(value), value);
    }
    @SuppressWarnings("unchecked")
    public <T> T getOption(NexalithicOption<T> option) {
        Object value = options.get(option);
        if (value == null) {
            return option.defaultValue(this);
        } else {
            return (T) value;
        }
    }

    /**
     * @param module 模块定义
     * @param value 实际注入的模块对象
     * @param <T> 模块声明的基础类型
     * @param <V> 实际注入类型，必须是 {@code T} 的子类型
     * @return 当前构建上下文
     */
    public <T, V extends T> NexalithicBuilderContext setModule(NexalithicModule<T> module, V value) {
        if (!module.type().isInstance(value)) {
            throw new IllegalArgumentException(String.format(
                    "Module [%s] mismatch: Expected %s, but got %s",
                    module.name(), module.type().getSimpleName(), value.getClass().getSimpleName()));
        }
        modules.put(module, value);
        return this;
    }

    /**
     * @param module 模块定义
     * @param <T> Key 定义时携带的原始类型
     * @param <V> 调用方要求的具体子类型
     */
    @SuppressWarnings("unchecked")
    public <T, V extends T> V getModule(NexalithicModule<T> module) {
        Object value = modules.get(module);
        if (value == null) {
            throw new IllegalArgumentException("Module " + module.name() + " not found");
        }
        if (!module.type().isInstance(value)) {
            throw new IllegalArgumentException(String.format(
                    "Module [%s] mismatch: Expected %s, but got %s",
                    module.name(), module.type().getSimpleName(), value.getClass().getSimpleName()));
        }
        return (V) value;
    }
    @SuppressWarnings("unchecked")
    public <T, V extends T> V getModule(NexalithicModule<T> module, Supplier<? extends T> lazy) {
        Object value = modules.get(module);
        if (value == null) {
            synchronized (modules) {
                value = modules.get(module);
                if (value == null) {
                    value = lazy.get();
                    setModule(module, (V) value);
                }
            }
        }
        if (!module.type().isInstance(value)) {
            throw new IllegalArgumentException(String.format(
                    "Module [%s] mismatch: Expected %s, but got %s",
                    module.name(), module.type().getSimpleName(), value.getClass().getSimpleName()));
        }
        return (V) value;
    }

    @SuppressWarnings("unchecked")
    public <T> T getConstant(Class<?> holder, Class<T> constant, Supplier<T> lazy) {
        CompositeKey key = new CompositeKey(holder, constant);
        Object value = constants.get(key);
        if (value == null) {
            synchronized (constants) {
                value = constants.get(key);
                if (value == null) {
                    value = lazy.get();
                    constants.put(key, value);
                }
            }
        }
        return (T) value;
    }

    public int nextOrdinal(Class<?> namespace, Class<?> type) {
        return ordinals.computeIfAbsent(new CompositeKey(namespace, type), ignored -> new AtomicInteger()).getAndIncrement();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<NexalithicOption<?>, Object> entry : options.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    private record CompositeKey(Class<?> holder, Class<?> type) {}
}
