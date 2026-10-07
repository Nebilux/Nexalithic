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

package com.nebilux.nexalithic.server.io.accept;

import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;

import java.nio.channels.SocketChannel;

/**
 * 准入任务执行入口。
 *
 * <p>该接口由 {@link AdmissionStrategy} 实现并提供给
 * {@link AdmissionDispatcher}。Dispatcher 只负责选择任务的执行线程和执行时机，
 * 实际的上下文获取、过滤链执行、异常传播以及连接移交仍由 AdmissionStrategy 完成。</p>
 *
 * <p>Dispatcher 接收任务后必须保证该入口最多执行一次，避免同一个连接被重复过滤、
 * 重复关闭或重复移交给握手阶段。</p>
 *
 * @author Reonvia
 * @since 0.2.0
 */
@FunctionalInterface
public interface AdmissionExecutor {

    /**
     * 执行一次连接准入任务。
     *
     * @param kind    待准入连接的通道种类
     * @param channel 待准入的通道
     */
    void execute(NexalithicChannel.Kind kind, SocketChannel channel);
}