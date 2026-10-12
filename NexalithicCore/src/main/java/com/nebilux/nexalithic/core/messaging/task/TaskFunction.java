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

package com.nebilux.nexalithic.core.messaging.task;

import com.nebilux.nexalithic.core.messaging.task.future.TaskFuture;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 业务任务函数接口，定义了一个异步请求-响应周期的完整生命周期。
 * <p>
 * 该接口作为 Nexalithic 框架中“请求意图”的抽象，封装了从数据包构造到结果异步回执的所有行为。
 * </p>
 * <b>生命周期流向：</b>
 * <pre>
 * [提交任务] -> request() -> (网络传输) -> [等待响应/超时/异常]
 * |--> 正常响应: response() -> complete() -> finish()
 * |--> 触发超时: timeout() -> finish()
 * |--> 主动取消: cancel() -> finish()
 * |--> 运行异常: failed() -> finish()
 * </pre>
 *
 * failed()仅在request()/response()产生异常时才触发
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface TaskFunction {
    @FunctionalInterface
    interface RequestAction extends TaskFunction {
        BusinessPacket execute();
    }
    @FunctionalInterface
    interface ResponseAction extends TaskFunction {
        void execute(BusinessPacket packet, TaskFuture future);
    }
    @FunctionalInterface
    interface SimpleResponseAction extends TaskFunction {
        void execute(BusinessPacket packet);
    }
    @FunctionalInterface
    interface CompleteAction extends TaskFunction {
        void execute();
    }
    @FunctionalInterface
    interface TimeoutAction extends TaskFunction {
        void execute();
    }
    @FunctionalInterface
    interface CancelAction extends TaskFunction {
        void execute();
    }
    @FunctionalInterface
    interface FailedAction extends TaskFunction {
        void execute(Exception e);
    }
    @FunctionalInterface
    interface FinishAction extends TaskFunction {
        void execute();
    }
}