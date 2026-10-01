package com.nebilux.nexalithic.core.model.stream;

import com.nebilux.nexalithic.core.model.AbstractModel;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.stream.chunk.AbstractChunk;

/**
 * 抽象流
 *
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class AbstractStream<C extends AbstractChunk> implements AbstractModel {
    public enum StreamType {
        /** 媒体 */ Media,
        /** 文件 */ File
    }

    /**
     * 获取流类型
     *
     * @return {@link AbstractPacket.PacketType }
     */
    public abstract StreamType streamType();
    public final ModelType modelType() {
        return ModelType.Stream;
    }
}
