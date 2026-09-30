package com.nebilux.nexalithic.core.messaging.payload;

import com.nebilux.nexalithic.core.model.packet.business.payload.AbstractPayload;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Payload 构造器存储策略接口。
 * <p>
 * 该接口定义了 Payload 元数据的存储与检索契约。设计采用“配置-冻结”模式：
 * 1. 启动期：通过 {@link #freeze(Map)} 将扫描到的原始映射关系转化为高性能的内部结构。
 * 2. 运行期：通过 {@link #get(long)} 进行检索，此时实现必须保证线程安全且不可变。
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/03/28
 */
public interface PayloadConstructorStorage {

    /**
     * 冻结存储器并构建不可变索引。
     * <p>
     * 该方法由 Registry 的 Builder 在初始化完成前调用。
     * 实现类应在此阶段根据 Map 的特征（如 UID 的离散程度）决定底层的存储数据结构（如 Array 或 Map）。
     * 执行此方法后，存储器应进入“只读”模式。
     * </p>
     *
     * @param map 原始的 UID 与 Payload 构造器的映射表
     * @return 冻结后的存储器实例（通常返回 this）
     */
    PayloadConstructorStorage freeze(Map<Long, Supplier<? extends AbstractPayload<?>>> map);

    /**
     * 根据 Payload UID 获取对应的构造器。
     * <p>
     * <b>性能要求：</b> 此方法在 I/O 线程高频调用，实现类应追求 O(1) 的时间复杂度。
     * <b>线程安全：</b> 在执行过 freeze 后，此方法必须支持多线程并发无锁读取。
     * </p>
     *
     * @param UID Payload 的唯一标识符
     * @return 对应的 Payload 构造器；若未注册则返回 {@code null}
     */
    Supplier<? extends AbstractPayload<?>> get(long UID);

    /**
     * 获取当前存储器中注册的所有构造器集合。
     *
     * @return 存储器中所有构造器的不可变集合
     */
    Collection<Supplier<? extends AbstractPayload<?>>> all();

    static PayloadConstructorStorage toArray(Map<Long, Supplier<? extends AbstractPayload<?>>> map) {
        return new ArrayPayloadStorage().freeze(map);
    }

    static PayloadConstructorStorage toMap(Map<Long, Supplier<? extends AbstractPayload<?>>> map) {
        return new MapPayloadStorage().freeze(map);
    }

    class ArrayPayloadStorage implements PayloadConstructorStorage {
        private Supplier<? extends AbstractPayload<?>>[] table;

        @Override
        @SuppressWarnings("unchecked")
        public PayloadConstructorStorage freeze(Map<Long, Supplier<? extends AbstractPayload<?>>> map) {
            long maxUid = map.keySet().stream().max(Long::compare).orElse(0L);
            // 建立一个固定长度的数组，UID 即下标
            this.table = new Supplier[(int) (maxUid + 1)];
            map.forEach((uid, supplier) -> table[uid.intValue()] = supplier);
            return this; // 返回当前实例（已冻结）
        }

        @Override
        public Supplier<? extends AbstractPayload<?>> get(long UID) {
            if (UID < 0 || UID >= table.length) return null;
            return table[(int) UID];
        }

        @Override
        public Collection<Supplier<? extends AbstractPayload<?>>> all() {
            return Arrays.stream(table).filter(Objects::nonNull).toList();
        }
    }

    class MapPayloadStorage implements PayloadConstructorStorage {
        private Map<Long, Supplier<? extends AbstractPayload<?>>> map;

        @Override
        public PayloadConstructorStorage freeze(Map<Long, Supplier<? extends AbstractPayload<?>>> map) {
            this.map = Map.copyOf(map);
            return this;
        }

        @Override
        public Supplier<? extends AbstractPayload<?>> get(long UID) {
            return map.get(UID);
        }

        @Override
        public Collection<Supplier<? extends AbstractPayload<?>>> all() {
            return map.values();
        }
    }
}
