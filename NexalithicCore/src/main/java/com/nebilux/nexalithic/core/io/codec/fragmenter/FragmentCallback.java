package com.nebilux.nexalithic.core.io.codec.fragmenter;

import com.nebilux.nexalithic.core.io.codec.TaskCodecCallback;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferListener;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 片段回调
 *
 * @author Reonvia
 * @since 0.2.0
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
