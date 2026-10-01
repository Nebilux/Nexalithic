package com.nebilux.nexalithic.core.io.codec;

import com.nebilux.nexalithic.core.messaging.task.NexalithicTask;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferListener;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;

/**
 * 任务编解码器回调
 *
 * @author Reonvia
 * @since 0.2.0
 */
public abstract class TaskCodecCallback implements CodecCallback {
    protected final TaskScheduler taskScheduler;
    protected NexalithicSession<?> session;
    protected NexalithicTask task;
    protected TransferListener transferListener;
    protected TransferSnapshot transferSnapshot;
    private boolean completed;

    protected TaskCodecCallback(TaskScheduler scheduler) {
        this.taskScheduler = scheduler;
    }

    @Override
    public void bind(NexalithicSession<?> session) {
        this.session = session;
    }

    @Override
    public void prepare(long taskId, BusinessPacket.Way way) {
        if (!accept(way)) {
            return;
        }
        task = session == null ? null : session.getTaskCoordinator().find(taskId);
        transferListener = task == null ? null : transferListener();
    }

    @Override
    public void start(long total) {
        if (transferListener == null || transferSnapshot != null || completed) {
            return;
        }
        transferSnapshot = new TransferSnapshot(total);
        if (!taskScheduler.start(transferListener, transferSnapshot)) {
            transferSnapshot = null;
        }
    }

    @Override
    public void update(long remaining) {
        if (transferSnapshot != null && !completed) {
            transferSnapshot.updateRemaining(remaining);
        }
    }

    @Override
    public final void complete() {
        if (completed) {
            return;
        }
        completed = true;
        if (transferSnapshot != null) {
            transferSnapshot.updateRemaining(0);
            taskScheduler.finish(transferListener);
        }
        onComplete();
    }

    @Override
    public void clear() {
        if (!completed && transferSnapshot != null) {
            taskScheduler.cancel(transferListener);
        }
        session = null;
        task = null;
        transferListener = null;
        transferSnapshot = null;
        completed = false;
    }

    protected abstract TransferListener transferListener();

    protected abstract boolean accept(BusinessPacket.Way way);

    protected void onComplete() {}
}
