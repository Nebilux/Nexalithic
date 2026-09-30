package com.nebilux.nexalithic.core.io.codec.fragmenter;

import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.recyclable.WrapperPool;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务数据包碎片器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/10
 * @version 1.0.0
 */
public class BusinessPacketsFragmenter implements PacketsFragmenter<BusinessPacket> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, BusinessPacketsFragmenter.class);
    public static final class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> WrapperQueue_Capacity = defineOption(
                64, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> WrapperLinked_Capacity = defineOption(
                64, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> WrapperPool_Capacity = defineOption(
                4096, OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(BusinessPacketsFragmenter.class);
    private final WrapperPool<BusinessPacketFragmentWrapper> wrapperPool;
    private final NexalithicSession<?> owner;
    private final int WrapperLinked_Capacity_;
    private final MpscArrayQueue<BusinessPacketFragmentWrapper> packets;
    private final AtomicInteger currentLinkedCount = new AtomicInteger(0);
    private BusinessPacketFragmentWrapper head, last;

    public BusinessPacketsFragmenter(NexalithicSession<?> owner,
                                     WrapperPool<BusinessPacketFragmentWrapper> wrapperPool,
                                     int WrapperQueue_Capacity_, int WrapperLinked_Capacity_) {
        this.owner = owner;
        this.wrapperPool = wrapperPool;
        this.WrapperLinked_Capacity_ = WrapperLinked_Capacity_;
        packets = new MpscArrayQueue<>(WrapperQueue_Capacity_);
    }

    @Override
    public boolean feed(BusinessPacket packet) {
        if (currentLinkedCount.get() >= WrapperLinked_Capacity_) {
            return false;
        }
        BusinessPacketFragmentWrapper wrapper = wrapperPool.acquire();
        if (wrapper == null) {
            return false;
        }
        wrapper.getCodecCallback().bind(owner);
        wrapper.wrap(packet.seal());
        if (!packets.offer(wrapper)) {
            wrapper.recycle();
            return false;
        }
        return true;
    }

    @Override
    public int fill(BusinessPacket... packets) {
        int count = packets.length;
        for (BusinessPacket packet : packets) {
            if (feed(packet)) {
                count--;
            } else {
                break;
            }
        }
        return count;
    }

    @Override
    public boolean drain(LoopBuffer target) throws IOException {
        int flag = target.writableBytes(), written;
        BusinessPacketFragmentWrapper wrapper;
        while ((wrapper = packets.peek()) != null) {
            written = wrapper.firstFrame(target);
            if (written == 0) {
                break;
            }
            packets.poll();
            if (wrapper.hasFrame()) {
                if (head == null) {
                    head = wrapper;
                    last = wrapper;
                }
                head.setPrev(wrapper);
                currentLinkedCount.incrementAndGet();
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("sent BUSINESS packet [{}]", wrapper.unwrap());
                }
                wrapper.recycle();
            }
        }
        wrapper = last;
        while (wrapper != null) {
            written = wrapper.nextFrame(target);
            if (written == 0) {
                break;
            }
            if (wrapper.hasFrame()) {
                wrapper = wrapper.getNext();
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("sent BUSINESS packet [{}]", wrapper.unwrap());
                }
                BusinessPacketFragmentWrapper next = wrapper.removeSelfAndGetNext();
                currentLinkedCount.decrementAndGet();
                if (next == null) {
                    wrapper.recycle();
                    head = null;
                    last = null;
                    break;
                }
                if (wrapper == head) {
                    head = next;
                }
                wrapper.recycle();
                wrapper = next;
            }
            last = wrapper;
        }
        return flag != target.writableBytes();
    }

    @Override
    public boolean isEmpty() {
        return packets.isEmpty() && currentLinkedCount.get() == 0;
    }

    @Override
    public void clear() {
        BusinessPacketFragmentWrapper queued;
        while ((queued = packets.poll()) != null) {
            queued.recycle();
        }
        BusinessPacketFragmentWrapper wrapper = head;
        while (wrapper != null) {
            BusinessPacketFragmentWrapper next = wrapper.removeSelfAndGetNext();
            wrapper.recycle();
            wrapper = next;
        }
        currentLinkedCount.set(0);
        head = null;
        last = null;
    }
}
