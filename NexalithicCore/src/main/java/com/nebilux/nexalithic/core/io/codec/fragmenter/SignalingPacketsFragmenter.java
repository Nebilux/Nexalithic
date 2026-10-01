package com.nebilux.nexalithic.core.io.codec.fragmenter;

import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.model.packet.signaling.SignalingPacket;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 信令分组碎片器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class SignalingPacketsFragmenter implements PacketsFragmenter<SignalingPacket> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, SignalingPacketsFragmenter.class);
    public static final class Options extends OptionsDefinition {
        public final NexalithicOption<Integer> WrapperQueue_Capacity = defineOption(
                256, OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(SignalingPacketsFragmenter.class);
    private final MpscArrayQueue<SignalingPacket> packets;
    private SignalingPacket currentPacket;

    public SignalingPacketsFragmenter(int WrapperQueue_Capacity_) {
        packets = new MpscArrayQueue<>(WrapperQueue_Capacity_);
    }

    @Override
    public boolean feed(SignalingPacket wrapper) {
        return packets.offer(wrapper);
    }

    @Override
    public int fill(SignalingPacket... wrappers) {
        int count = wrappers.length;
        for (SignalingPacket wrapper : wrappers) {
            if (feed(wrapper)) {
                count--;
            } else {
                break;
            }
        }
        return count;
    }

    @Override
    public boolean drain(LoopBuffer target) {
        int flag = target.writableBytes();
        SignalingPacket packet = currentPacket;
        while (packet != null || !packets.isEmpty()) {
            if (packet == null) {
                packet = packets.poll();
                if (packet == null) {
                    return false;
                }
            }
            if (packet.toBuffer(target)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("sent SIGNALING packet [{}]", packet);
                }
                packet = null;
            } else {
                currentPacket = packet;
                break;
            }
        }
        return flag != target.writableBytes();
    }

    @Override
    public boolean isEmpty() {
        return currentPacket == null && packets.isEmpty();
    }

    @Override
    public void clear() {
        packets.clear();
        currentPacket = null;
    }
}
