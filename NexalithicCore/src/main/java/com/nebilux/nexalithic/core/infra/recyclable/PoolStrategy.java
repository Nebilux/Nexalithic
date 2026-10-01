package com.nebilux.nexalithic.core.infra.recyclable;

/**
 * 池资源获取策略
 *
 * @author Reonvia
 * @since 0.1.0
 */
public interface PoolStrategy {
    enum Permit {
        /**
         * 无配额限制
         */
        NONE,

        /**
         * 回收时必须归还
         */
        ACQUIRED,

        /**
         * 借债使用,回收时不能归还
         */
        BORROWED
    }

    Permit beforeAcquire();

    boolean allowCreate();

    void afterRelease(Permit permit);
}
