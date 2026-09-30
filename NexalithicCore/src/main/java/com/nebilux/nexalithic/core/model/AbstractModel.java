package com.nebilux.nexalithic.core.model;

/**
 * 抽象模型
 *
 * @author tbrtz647@outlook.com
 * @since 2026/03/03
 * @version 1.0.0
 */
public interface AbstractModel {
    int MAGIC_NUMBER = 0x494D5450;
    enum ModelType {
        Packet,
        Stream
    }

    ModelType modelType();
}
