/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.model.business.stream;

import com.nebilux.nexalithic.core.internal.model.stream.BusinessStreamImpl;
import com.nebilux.nexalithic.core.model.business.BusinessModel;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * 由完整块组成的业务流模型。
 * <p>
 * 方向以应用为参照：输入送往通道，输出来自通道，双向同时支持两者。
 * 用户通过发送器向输入流提交块，或通过接收器从输出流取块；
 * 不支持的用户方向返回 {@code null}。
 * 请求或响应方式与块方向相互独立，创建时一并指定。
 *
 * @param <C> 流块类型
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class BusinessStream<C extends BusinessStream.Chunk> extends BusinessModel<BusinessStream<C>> {
    /**
     * 流交付给上层的最小完整单位。
     *
     * <p>序号用于识别块的顺序或缺失。底层可以分片传输一个块，但不得向上层
     * 交付尚未重组完成的块。媒体时间戳、文件偏移及载荷由具体子类定义。</p>
     *
     * @author Reonvia
     * @since 0.1.0
     */
    public abstract static class Chunk implements BusinessModel.Payload {
        private final long sequence;

        protected Chunk(long sequence) {
            if (sequence < 0) {
                throw new IllegalArgumentException("sequence must be non-negative");
            }
            this.sequence = sequence;
        }

        public final long sequence() {
            return sequence;
        }

        /** 用于队列字节限额的块载荷字节数；同一块的返回值必须保持不变。 */
        public abstract int size();
    }

    /** 相对于应用的流方向：输入发送至通道，输出从通道交付给应用。 */
    public enum Direction {
        INPUT((byte) 0x01, true, false),
        OUTPUT((byte) 0x02, false, true),
        DUPLEX((byte) 0x03, true, true);

        private final byte code;
        private final boolean input;
        private final boolean output;

        Direction(byte code, boolean input, boolean output) {
            this.code = code;
            this.input = input;
            this.output = output;
        }

        public static Direction from(byte code) {
            return switch (code) {
                case 0x01 -> INPUT;
                case 0x02 -> OUTPUT;
                case 0x03 -> DUPLEX;
                default -> throw new IllegalStateException("Unexpected value: " + code);
            };
        }

        public byte code() {
            return code;
        }

        /** 返回对端应用所见方向；双向流保持双向。 */
        public Direction opposite() {
            return switch (this) {
                case INPUT -> OUTPUT;
                case OUTPUT -> INPUT;
                case DUPLEX -> DUPLEX;
            };
        }

        public boolean hasInput() {
            return input;
        }

        public boolean hasOutput() {
            return output;
        }
    }
    /**
     * 输入和输出队列各自的容量及满队列处理方式。
     * 预置策略的每个队列最多容纳 64 块、64 Mb；需要其他限额时可直接构造策略。
     */
    public record Policy(Sequence sequence, Buffering input, Buffering output) {
        private static final int DEFAULT_MAX_CHUNKS = 64;
        private static final long DEFAULT_MAX_BYTES = DEFAULT_MAX_CHUNKS * 1024 * 1024;

        /** 保持连续序号，队列满时等待容量。 */
        public static final Policy RELIABLE = preset(Sequence.CONTIGUOUS, OnFull.BACKPRESSURE);
        /** 优先保留新块，队列满时丢弃队首。 */
        public static final Policy PREFER_LATEST = preset(Sequence.INCREASING, OnFull.DROP_HEAD);
        /** 优先保留已排队的块，队列满时丢弃新块。 */
        public static final Policy PREFER_QUEUED = preset(Sequence.INCREASING, OnFull.DROP_INCOMING);

        private static Policy preset(Sequence sequence, OnFull onFull) {
            Buffering buffering = new Buffering(DEFAULT_MAX_CHUNKS, DEFAULT_MAX_BYTES, onFull);
            return new Policy(sequence, buffering, buffering);
        }

        public enum Sequence {
            CONTIGUOUS((byte) 0x01),   // 序号必须连续
            INCREASING((byte) 0x02);   // 序号递增，允许缺口

            private final byte code;

            Sequence(byte code) {
                this.code = code;
            }

            public byte code() {
                return code;
            }

            public static Sequence from(byte code) {
                return switch (code) {
                    case 0x01 -> CONTIGUOUS;
                    case 0x02 -> INCREASING;
                    default -> throw new IllegalArgumentException("Unknown value: " + Byte.toUnsignedInt(code));
                };
            }
        }

        /** 队列满时如何处理新块；队首和队尾均指已排队的块。 */
        public enum OnFull {
            BACKPRESSURE((byte) 0x01),     // 保留已有块，等待容量
            DROP_HEAD((byte) 0x02),        // 丢弃队首已排队的最旧块，再加入新块
            DROP_TAIL((byte) 0x03),        // 丢弃队尾已排队的最新块，再加入新块
            DROP_INCOMING((byte) 0x04);    // 保留整个队列，丢弃新到达的块

            private final byte code;

            OnFull(byte code) {
                this.code = code;
            }

            public byte code() {
                return code;
            }

            public static OnFull from(byte code) {
                return switch (code) {
                    case 0x01 -> BACKPRESSURE;
                    case 0x02 -> DROP_HEAD;
                    case 0x03 -> DROP_TAIL;
                    case 0x04 -> DROP_INCOMING;
                    default -> throw new IllegalArgumentException("Unknown value: " + Byte.toUnsignedInt(code));
                };
            }
        }

        /** 块数与字节数任一达到限制，即按 {@code onFull} 处理。 */
        public record Buffering(int maxChunks, long maxBytes, OnFull onFull) {
            public Buffering {
                if (maxChunks <= 0 || maxBytes <= 0) {
                    throw new IllegalArgumentException("Buffer limits must be positive");
                }
                requireNonNull(onFull);
            }
        }

        public Policy {
            requireNonNull(sequence);
            requireNonNull(input);
            requireNonNull(output);
            if (sequence == Sequence.CONTIGUOUS
                    && (input.onFull() != OnFull.BACKPRESSURE
                    || output.onFull() != OnFull.BACKPRESSURE)) {
                throw new IllegalArgumentException("Dropping chunks conflicts with contiguous sequence");
            }
        }
    }

    private final Direction direction;
    private final Policy policy;
    private final Class<C> type;
    private final AtomicReference<RuntimeException> callbackFailure = new AtomicReference<>();

    protected BusinessStream(Class<C> type, Direction direction, Policy policy, Way way, short[] path, long correlationId) {
        super(way, verifyAndCopyPath(path), correlationId);
        this.type = requireNonNull(type, "type");
        this.direction = requireNonNull(direction, "direction");
        this.policy = requireNonNull(policy, "policy");
    }

    public static <C extends Chunk> BusinessStream<C> open(Class<C> type, Direction direction, Policy policy, Way way, short[] path, long correlationId) {
        return new BusinessStreamImpl<>(type, direction, policy, way, path, correlationId);
    }

    public static <C extends Chunk> BusinessStream<C> open(Class<C> type, Direction direction, Policy policy, Way way, short... path) {
        return new BusinessStreamImpl<>(type, direction, policy, way, path, BusinessModel.DEFAULT_CORRELATION_ID);
    }

    public static <C extends Chunk> Builder<C> builder(Class<C> type, Direction direction, Policy policy, Way way, short... path) {
        return new Builder<>(type, direction, policy, way, path);
    }

    /**
     * 返回供用户向输入流提交块的发送器。
     *
     * @return 发送器；当前流不支持发送时返回 {@code null}
     */
    public abstract StreamFeeder<C> feeder();

    /**
     * 返回供用户从输出流取块的接收器。
     *
     * @return 接收器；当前流不支持接收时返回 {@code null}
     */
    public abstract StreamDrainer<C> drainer();

    public final Direction direction() {
        return direction;
    }

    public final Policy policy() {
        return policy;
    }

    public final Class<C> type() {
        return type;
    }

    /**
     * 返回首次通知回调抛出的异常；没有发生过时返回 {@code null}。
     * 通知失败不会撤销已经完成的队列操作，也不会自动结束流。
     */
    public final RuntimeException getCallbackFailure() {
        return callbackFailure.get();
    }

    protected final void recordCallbackFailure(RuntimeException failure) {
        callbackFailure.compareAndSet(null, requireNonNull(failure, "failure"));
    }

    @Override
    public Form form() {
        return Form.Stream;
    }

    /**
     * 可复用的流构建器。每次构建均创建独立的流与块队列。
     * 所有配置在创建构建器时固定，因此 {@link #reset()} 无需清理逐流状态。
     *
     * @param <C> 流块类型
     *
     * @author Reonvia
     * @since 0.5.0
     */
    public static class Builder<C extends Chunk> extends BusinessModel.Builder<BusinessStream<C>> {
        private final Class<C> type;
        private final Direction direction;
        private final Policy policy;

        protected Builder(Class<C> type, Direction direction, Policy policy, Way way, short... path) {
            super(way, path);
            this.type = requireNonNull(type, "type");
            this.direction = requireNonNull(direction, "direction");
            this.policy = requireNonNull(policy, "policy");
        }

        @Override
        public void reset() {}

        @Override
        public BusinessStream<C> build(long correlationId) {
            return BusinessStream.open(type, direction, policy, way, path, correlationId);
        }
    }
}
