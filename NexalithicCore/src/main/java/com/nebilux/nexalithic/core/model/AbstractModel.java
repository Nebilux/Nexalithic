package com.nebilux.nexalithic.core.model;

/**
 * 抽象模型
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface AbstractModel {
    int MAGIC_NUMBER = 0x494D5450;
    enum ModelType {
        Packet,
        Stream
    }

    ModelType modelType();
}
