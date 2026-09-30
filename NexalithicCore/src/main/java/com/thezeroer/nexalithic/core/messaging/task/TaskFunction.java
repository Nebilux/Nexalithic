package com.thezeroer.nexalithic.core.messaging.task;

import com.thezeroer.nexalithic.core.messaging.task.future.TaskFuture;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;

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
 * @author tbrtz647@outlook.com
 * @since 2026/03/21
 * @version 1.0.0
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