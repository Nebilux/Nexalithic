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

package com.nebilux.nexalithic.core.model.stream;

import com.nebilux.nexalithic.core.model.NexalithicModel;

import java.util.Objects;

/**
 * 由完整块组成的逻辑流的公开模型。框架内部状态保存块及收发进度。
 *
 * 方向以应用为参照：输入送往通道，输出来自通道，双向同时支持两者。
 * 用户通过发送器向输入流提交块，或通过接收器从输出流取块；
 * 不支持的用户方向返回 {@code null}。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class AbstractStream<C extends AbstractChunk> implements NexalithicModel {
    public enum Type {
        /** 媒体 */ Media,
        /** 文件 */ File
    }

    /** 相对于应用的流方向：输入发送至通道，输出从通道交付给应用。 */
    public enum Direction {
        INPUT(true, false),
        OUTPUT(false, true),
        DUPLEX(true, true);

        private final boolean input;
        private final boolean output;

        Direction(boolean input, boolean output) {
            this.input = input;
            this.output = output;
        }

        public boolean hasInput() {
            return input;
        }

        public boolean hasOutput() {
            return output;
        }
    }

    private final Type type;
    private final Direction direction;

    protected AbstractStream(Type type, Direction direction) {
        this.type = Objects.requireNonNull(type, "type");
        this.direction = Objects.requireNonNull(direction, "direction");
    }

    public final Type type() {
        return type;
    }

    public final Direction direction() {
        return direction;
    }

    /**
     * 返回供用户向输入流提交块的发送器。
     *
     * @return 发送器；当前流不支持发送时返回 {@code null}
     */
    public ChunkSender<C> sender() {
        return null;
    }

    /**
     * 返回供用户从输出流取块的接收器。
     *
     * @return 接收器；当前流不支持接收时返回 {@code null}
     */
    public ChunkReceiver<C> receiver() {
        return null;
    }

    @Override
    public final Model model() {
        return Model.Stream;
    }
}
