package com.nebilux.nexalithic.core.io.codec.assembler;

import com.nebilux.nexalithic.core.builder.module.ModulesDefinition;
import com.nebilux.nexalithic.core.builder.module.NexalithicModule;
import com.nebilux.nexalithic.core.builder.option.NexalithicOption;
import com.nebilux.nexalithic.core.builder.option.OptionValidator;
import com.nebilux.nexalithic.core.builder.option.OptionsDefinition;
import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;
import com.nebilux.nexalithic.core.infra.recyclable.WrapperPool;
import com.nebilux.nexalithic.core.infra.timer.TimeWheel;
import com.nebilux.nexalithic.core.infra.timer.TimerContext;
import com.nebilux.nexalithic.core.infra.timer.TimerCoordinator;
import com.nebilux.nexalithic.core.io.codec.PacketFrame;
import com.nebilux.nexalithic.core.messaging.payload.PayloadRegistry;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;
import com.nebilux.nexalithic.core.session.NexalithicSession;
import org.jctools.queues.MpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 业务包汇编器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/02/10
 * @version 1.0.0
 */
public class BusinessPacketsAssembler implements PacketsAssembler<BusinessPacket>, TimerCoordinator<BusinessPacketAssemblyWrapper> {
    public static final Options OPTIONS = OptionsDefinition.initOptions(Options.class, BusinessPacketsAssembler.class);
    public static final class Options extends OptionsDefinition {
        public final TimeWheel.Options TimeWheel = new TimeWheel.Options(holder) {
            protected NexalithicOption<Integer> SlotCount() {
                return defineOptionLazy(context ->
                                Math.toIntExact(context.getOption(OPTIONS.MaxIdleTimeMillis) / context.getOption(OPTIONS.TimeWheel.TickMillis)) + 1
                        , OptionValidator.positive()
                );
            }
        };
        public final NexalithicOption<Integer> WrapperPool_Capacity = defineOption(
                1024, OptionValidator.positive()
        );
        public final NexalithicOption<Integer> PacketQueue_Capacity = defineOption(
                64, OptionValidator.positive()
        );
        public final NexalithicOption<Long> MaxIdleTimeMillis = defineOption(
                30_000L, OptionValidator.positive()
        );
        private Options(Class<?> holder) {
            super(holder);
        }
    }
    public static final Modules MODULES = new Modules();
    public static final class Modules extends ModulesDefinition {
        public final NexalithicModule<PayloadRegistry> PayloadRegistry = defineModule(PayloadRegistry.class);
        public final NexalithicModule<TimeWheel<BusinessPacketAssemblyWrapper>> TimeWheel = defineModule(TimeWheel.class);
        private Modules() {
            super(BusinessPacketsAssembler.class);
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(BusinessPacketsAssembler.class);
    private final NexalithicSession<?> owner;
    private final Map<Integer, BusinessPacketAssemblyWrapper> assemblingMap;
    private final MpscArrayQueue<BusinessPacket> completedPackets;
    private BusinessPacket pendingPacket;

    private final WrapperPool<BusinessPacketAssemblyWrapper> wrapperPool;
    private final TimeWheel<BusinessPacketAssemblyWrapper> timeWheel;

    public BusinessPacketsAssembler(NexalithicSession<?> owner, WrapperPool<BusinessPacketAssemblyWrapper> wrapperPool, TimeWheel<BusinessPacketAssemblyWrapper> timeWheel, int PacketQueue_Capacity_) {
        this.owner = owner;
        this.wrapperPool = wrapperPool;
        this.timeWheel = timeWheel;
        assemblingMap = new ConcurrentHashMap<>();
        completedPackets = new MpscArrayQueue<>(PacketQueue_Capacity_);
    }

    @Override
    public boolean feed(LoopBuffer source) throws IOException {
        int flag = source.readableBytes();
        if (pendingPacket != null) {
            if (completedPackets.offer(pendingPacket)) {
                pendingPacket = null;
            } else {
                return false;
            }
        }
        int read;
        while (source.readableBytes() > PacketFrame.FRAME_HEADER_LENGTH) {
            source.markHead();
            long meta = source.unsafeGetLong();
            int payloadLength = PacketFrame.parsePayloadLength(meta);
            int packetId = PacketFrame.parsePacketId(meta);
            if (source.readableBytes() < payloadLength) {
                source.resetHead();
                break;
            }
            BusinessPacketAssemblyWrapper wrapper = assemblingMap.get(packetId);
            if (wrapper == null) {
                wrapper = wrapperPool.acquire().setPacketId(packetId);
                wrapper.getCodecCallback().bind(owner);
                wrapper.updateLastActiveTime();
                assemblingMap.put(packetId, wrapper);
                timeWheel.schedule(wrapper, wrapper.stamp(), this);
            }
            read = wrapper.onFrame(source, payloadLength, PacketFrame.isStart(meta));
            if (!wrapper.hasFrame()) {
                assemblingMap.remove(packetId);
                BusinessPacket packet = wrapper.getPacket();
                if (logger.isTraceEnabled()) {
                    logger.trace("received BUSINESS packet [{}]", packet);
                }
                wrapper.recycle();
                if (!completedPackets.offer(packet)) {
                    pendingPacket = packet;
                    break;
                }
            }
            if (read == 0) {
                break;
            }
        }
        return flag != source.readableBytes();
    }

    @Override
    public BusinessPacket drain() {
        if (pendingPacket != null && completedPackets.offer(pendingPacket)) {
            pendingPacket = null;
        }
        return completedPackets.poll();
    }

    @Override
    public void clear() {
        pendingPacket = null;
        completedPackets.clear();
        for (BusinessPacketAssemblyWrapper wrapper : assemblingMap.values()) {
            wrapper.recycle();
        }
        assemblingMap.clear();
    }

    @Override
    public long getExpiryTimeNanos(TimerContext<BusinessPacketAssemblyWrapper> context) {
        return context.target().getExpiryTimeNanos();
    }

    @Override
    public boolean isCancelled(TimerContext<BusinessPacketAssemblyWrapper> context) {
        return !context.target().isActive(context.targetStamp());
    }

    @Override
    public boolean onExpiryTrigger(TimerContext<BusinessPacketAssemblyWrapper> context) {
        BusinessPacketAssemblyWrapper target = context.target();
        if (!target.isActive(context.targetStamp())) {
            return true;
        }
        if (System.nanoTime() < target.getExpiryTimeNanos()) {
            return false;
        }
        assemblingMap.remove(target.getPacketId());
        target.recycle();
        return true;
    }
}
