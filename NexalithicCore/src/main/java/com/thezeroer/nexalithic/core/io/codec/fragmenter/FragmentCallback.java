package com.thezeroer.nexalithic.core.io.codec.fragmenter;

import com.thezeroer.nexalithic.core.io.codec.TaskCodecCallback;
import com.thezeroer.nexalithic.core.messaging.task.TaskScheduler;
import com.thezeroer.nexalithic.core.messaging.task.visual.TransferListener;
import com.thezeroer.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 片段回调
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/08/16
 */
public class FragmentCallback extends TaskCodecCallback {
    public FragmentCallback(TaskScheduler scheduler) {
        super(scheduler);
    }

    @Override
    protected boolean accept(BusinessPacket.Way way) {
        return way.isRequest();
    }

    @Override
    protected TransferListener transferListener() {
        return task.getRequestListener();
    }

    @Override
    protected void onComplete() {
        if (session != null && task != null) {
            session.getTaskCoordinator().activate(task);
        }
    }
}
