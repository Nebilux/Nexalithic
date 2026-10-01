package com.nebilux.nexalithic.core.io.codec.assembler;

import com.nebilux.nexalithic.core.io.codec.TaskCodecCallback;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferListener;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 装配回调
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class AssemblyCallback extends TaskCodecCallback {
    public AssemblyCallback(TaskScheduler scheduler) {
        super(scheduler);
    }

    @Override
    protected boolean accept(BusinessPacket.Way way) {
        return way.isResponse();
    }

    @Override
    protected TransferListener transferListener() {
        return task.getResponseListener();
    }
}
