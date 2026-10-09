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

package com.nebilux.nexalithic.core.lifecycle;

import java.util.concurrent.CompletionStage;

/**
 * 可启动、可关闭组件的生命周期接口。
 *
 * <p>操作方法返回的 {@link CompletionStage} 表示操作的最终结果，而不是仅表示请求已提交。
 * {@link #lifecycleState()} 只报告进度；启动或关闭是否失败，应检查相应的完成阶段。
 * 在任何完成阶段上注册的非异步回调都可能由完成该阶段的线程直接执行，
 * 因此回调不应同步等待仍需由该线程推进的生命周期操作。</p>
 *
 * <p>重复调用的处理、启动与关闭并发时的具体转换，以及关闭钩子的调度顺序，
 * 由实现类约定。调用方不应仅凭状态推断某个异步操作已经完成。</p>
 *
 * @author Reonvia
 * @since 0.3.1
 */
public interface Lifecycle {
    /** 生命周期的可观察进度，不包含操作的成功或失败结果。 */
    enum State {
        /** 尚未接受启动或关闭请求。 */
        NEW,
        /** 启动过程尚未结束；此时也可能已经收到关闭请求。 */
        STARTING,
        /** 启动成功，组件处于运行状态。 */
        RUNNING,
        /** 已选择优雅关闭，但排空操作不一定已经开始或完成。 */
        QUIESCING,
        /** 正在终止；关闭钩子不一定已经开始或完成。 */
        TERMINATING,
        /** 终止流程已结束；是否成功仍须检查终止完成阶段。 */
        TERMINATED;

        /**
         * 判断状态是否为 {@link #TERMINATING} 或 {@link #TERMINATED}。
         *
         * <p>{@link #QUIESCING} 不属于此方法定义的“终止已开始”，
         * 尽管组件可能已经收到优雅关闭请求。</p>
         *
         * @return 仅当状态为 TERMINATING 或 TERMINATED 时返回 {@code true}
         */
        public boolean isTerminatingOrTerminated() {
            return this == TERMINATING || this == TERMINATED;
        }
    }

    /** 关闭策略；不表示相应关闭操作已经开始或完成。 */
    enum ShutdownMode {
        /** 尝试让已接收的工作完成后再关闭。 */
        DRAIN,
        /** 请求尽快停止并清理；具体中止行为由实现类提供。 */
        ABORT,
    }

    /**
     * 请求启动组件。
     *
     * @return 表示启动最终结果的阶段；启动失败或被关闭请求取消时异常完成
     */
    CompletionStage<Void> start();

    /**
     * 按指定策略请求关闭组件。
     *
     * @param mode 关闭策略，不可为 {@code null}
     * @return 表示终止流程最终结果的阶段；关闭失败时异常完成
     */
    CompletionStage<Void> shutdown(ShutdownMode mode);

    /**
     * 以 {@link ShutdownMode#DRAIN} 策略请求关闭。
     *
     * @return 表示终止流程最终结果的阶段
     */
    default CompletionStage<Void> shutdown() {
        return shutdown(ShutdownMode.DRAIN);
    }

    /**
     * 观察启动结果，不会自行发起启动。
     *
     * @return 启动完成阶段；若尚未请求启动或取消，阶段可以一直保持未完成
     */
    CompletionStage<Void> whenStarted();

    /**
     * 观察终止结果，不会自行发起关闭。
     *
     * @return 终止完成阶段；若尚未请求关闭，阶段可以一直保持未完成
     */
    CompletionStage<Void> whenTerminated();

    /**
     * 读取当前可观察状态。
     *
     * @return 当前生命周期状态；不能据此判断异步操作是否成功
     */
    State lifecycleState();
}
