package com.nebilux.nexalithic.core.infra.rate;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 高性能定点数速率限制器 (Fixed-Point RateLimiter)
 * 采用 Q32.32 格式：高32位存储字节整数，低32位存储小数部分。
 * 消除热点路径除法，支持最高 4GB/s 限速。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class RateLimiter {
    // 定点数转换常量：2^32
    private static final long FIXED_POINT_SHIFT = 32;
    private static final long SCALE = 1L << FIXED_POINT_SHIFT;
    private static final long NO_UPDATE = -2L;

    private final long burstCapacityFixed; // 定点化的最大容量

    // 每纳秒增加的定点信用值 (Q32.32)
    private long readIncrementPerNanoFixed = 0;
    private long writeIncrementPerNanoFixed = 0;

    // 当前剩余的定点信用值 (Q32.32)
    private long readCreditFixed;
    private long writeCreditFixed;

    private long lastRefillReadTime;
    private long lastRefillWriteTime;

    // 跨线程更新：高32位存储readRate，低32位存储writeRate (B/s)
    private final AtomicLong pendingReadRate = new AtomicLong(NO_UPDATE);
    private final AtomicLong pendingWriteRate = new AtomicLong(NO_UPDATE);

    /**
     * 构造函数：支持初始默认限速
     * @param burstCapacity 突发容量 (字节)
     * @param defaultReadRate 初始读限速 (字节/秒)
     * @param defaultWriteRate 初始写限速 (字节/秒)
     */
    public RateLimiter(long burstCapacity, long defaultReadRate, long defaultWriteRate) {
        long now = System.nanoTime();
        this.lastRefillReadTime = now;
        this.lastRefillWriteTime = now;
        this.burstCapacityFixed = burstCapacity << FIXED_POINT_SHIFT;
        this.readCreditFixed = burstCapacityFixed;
        this.writeCreditFixed = burstCapacityFixed;
        this.readIncrementPerNanoFixed = calculateIncrement(defaultReadRate);
        this.writeIncrementPerNanoFixed = calculateIncrement(defaultWriteRate);
    }

    public RateLimiter(long burstCapacity) {
        this(burstCapacity, -1L, -1L);
    }

    /**
     * 更新读速率
     * @param rate 字节/秒，-1为不限速
     */
    public void updateReadRate(long rate) {
        pendingReadRate.set(rate);
    }

    /**
     * 更新写速率
     * @param rate 字节/秒，-1为不限速
     */
    public void updateWriteRate(long rate) {
        pendingWriteRate.set(rate);
    }

    public void applyRate() {
        long now = System.nanoTime();
        long newRead = pendingReadRate.getAndSet(NO_UPDATE);
        long newWrite = pendingWriteRate.getAndSet(NO_UPDATE);
        if (newRead != NO_UPDATE) {
            this.readIncrementPerNanoFixed = calculateIncrement(newRead);
            this.lastRefillReadTime = now;
        }
        if (newWrite != NO_UPDATE) {
            this.writeIncrementPerNanoFixed = calculateIncrement(newWrite);
            this.lastRefillWriteTime = now;
        }
    }

    /**
     * 计算每纳秒的定点增量
     * @param rate 原始速率 (字节/秒)
     * @return 定点增量，-1 代表不限速
     */
    private long calculateIncrement(long rate) {
        if (rate >= 0xFFFFFFFFL || rate <= 0) {
            return -1L;
        }
        return (rate << FIXED_POINT_SHIFT) / 1_000_000_000L;
    }

    public void refillReadCredit() {
        long now = System.nanoTime();
        long delta = now - lastRefillReadTime;
        lastRefillReadTime = now;
        if (delta <= 0) {
            return;
        }
        if (readIncrementPerNanoFixed >= 0) {
            readCreditFixed = Math.min(burstCapacityFixed, readCreditFixed + delta * readIncrementPerNanoFixed);
        } else {
            readCreditFixed = burstCapacityFixed;
        }
    }

    public void refillWriteCredit() {
        long now = System.nanoTime();
        long delta = now - lastRefillWriteTime;
        lastRefillWriteTime = now;
        if (delta <= 0) {
            return;
        }
        if (writeIncrementPerNanoFixed >= 0) {
            writeCreditFixed = Math.min(burstCapacityFixed, writeCreditFixed + delta * writeIncrementPerNanoFixed);
        } else {
            writeCreditFixed = burstCapacityFixed;
        }
    }

    /**
     * 消耗读令牌
     */
    public void consumeRead(long bytes) {
        if (readIncrementPerNanoFixed >= 0) {
            readCreditFixed -= (bytes << FIXED_POINT_SHIFT);
        }
    }

    /**
     * 消耗写令牌
     */
    public void consumeWrite(long bytes) {
        if (writeIncrementPerNanoFixed >= 0) {
            writeCreditFixed -= (bytes << FIXED_POINT_SHIFT);
        }
    }

    public long getReadCredit() {
        if (readIncrementPerNanoFixed < 0) {
            return Integer.MAX_VALUE;
        }
        return readCreditFixed >>> FIXED_POINT_SHIFT;
    }

    public long getWriteCredit() {
        if (writeIncrementPerNanoFixed < 0) {
            return Integer.MAX_VALUE;
        }
        return writeCreditFixed >>> FIXED_POINT_SHIFT;
    }
}