/*
 * Copyright 2026 Nebilux
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebilux.nexalithic.core.messaging.task.visual;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 传输快照
 * 核心原理：利用 VarHandle 的 Release/Acquire 语义，实现 IO 线程写开销最小化，
 * 同时保证监控线程能读到一致的进度数据。
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class TransferSnapshot {
    private static final String[] SIZE_UNITS = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    private static final String[] SPEED_UNITS = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s", "PB/s", "EB/s"};
    private static final char[] SPIN_FRAMES = {'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'};

    private static final VarHandle VH;
    static {
        try {
            VH = MethodHandles.lookup().findVarHandle(TransferSnapshot.class, "transferred", long.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private final long total;
    private long transferred;

    private final long startTimeNanos;
    private long lastSnapshotTimeNanos;
    private long lastTransferred;
    private int lastFrameIndex;

    /** @param total 总字节数；-1 表示总量未知。 */
    public TransferSnapshot(long total) {
        if (total < -1) {
            throw new IllegalArgumentException("total must be non-negative or -1");
        }
        this.total = total;
        this.startTimeNanos = System.nanoTime();
        this.lastSnapshotTimeNanos = startTimeNanos;
        this.lastTransferred = 0;
    }

    /** 累加本次传输的字节数。 */
    public void appendTransferred(long increment) {
        VH.getAndAddRelease(this, increment);
    }

    public long getTotal() {
        return total;
    }
    public long getTransferred() {
        return (long) VH.getAcquire(this);
    }

    /**
     * 计算自开始以来的平均速度 (Bytes/s)
     */
    public double getAverageSpeed() {
        long durationNanos = System.nanoTime() - startTimeNanos;
        if (durationNanos <= 0) {
            return 0;
        }
        return (getTransferred() * 1_000_000_000.0) / durationNanos;
    }

    /**
     * 计算瞬时速度 (最近两次轮询间的速度)
     */
    public double getInstantSpeed() {
        long currentTimeNanos = System.nanoTime();
        long currentTransferred = getTransferred();
        long timeDelta = currentTimeNanos - lastSnapshotTimeNanos;
        if (timeDelta <= 0) {
            return 0;
        }
        long bytesDelta = currentTransferred - lastTransferred;
        double speed = (bytesDelta * 1_000_000_000.0) / timeDelta;
        this.lastSnapshotTimeNanos = currentTimeNanos;
        this.lastTransferred = currentTransferred;
        return Math.max(0, speed);
    }

    /**
     * 预计剩余时间 (秒)
     * 基于瞬时速度计算，如果瞬时速度为0则回退到平均速度
     */
    public long getEstimateArrivalTime() {
        if (total < 0) {
            return -1;
        }
        long remaining = Math.max(0, total - getTransferred());
        if (remaining == 0) {
            return 0;
        }
        double speed = getInstantSpeed();
        if (speed <= 0) {
            speed = getAverageSpeed();
        }
        if (speed <= 0) {
            return -1;
        }
        return (long) (remaining / speed);
    }

    /** 获取当前进度百分比（0.00 至 100.00）；总量未知时返回 -1。 */
    public double getPercent() {
        if (total < 0) return -1;
        if (total == 0) return 100.0;
        double percent = (getTransferred() * 100.0) / total;
        return Math.clamp(percent, 0.0, 100.0);
    }

    /**
     * 格式化当前进度百分比字符串
     */
    public String formatPercent() {
        return total < 0 ? "--" : String.format("%.2f%%", getPercent());
    }

    /**
     * 获取格式化的进度描述 (e.g. {@code "1.24 MB / 10.00 MB"})；总量未知时以 ? 表示总量。
     */
    public String getProgressStatus() {
        return formatSize(getTransferred()) + " / " + (total < 0 ? "?" : formatSize(total));
    }

    /**
     * 生成默认长度（100格）的文本进度条 (e.g. {@code [##########----------]})；总量未知时显示旋转指示符。
     */
    public String getProgressBar() {
        return getProgressBar(100, '#', '-', SPIN_FRAMES);
    }

    /**
     * 自定义文本进度条
     * @param width 进度条总宽度（字符数）
     * @param completedChar 已完成部分的字符
     * @param remainingChar 未完成部分的字符
     * @param spinFrames    动画帧序列 (传入 null 则不显示动态点)
     * @return 格式化后的进度条字符串
     */
    public String getProgressBar(int width, char completedChar, char remainingChar, char[] spinFrames) {
        double percent = getPercent();
        long transferred = getTransferred();
        int completedWidth = (int) (width * (percent / 100.0));
        completedWidth = Math.clamp(completedWidth, 0, width);
        StringBuilder sb = new StringBuilder(width + 2);
        sb.append('[');
        boolean showSpin = spinFrames != null && spinFrames.length > 0 && (total < 0 || transferred > 0 && percent < 100.0);
        int fillWidth = showSpin ? Math.min(completedWidth, width - 1) : completedWidth;
        sb.repeat(String.valueOf(completedChar), Math.max(0, fillWidth));
        if (showSpin) {
            char frame = spinFrames[lastFrameIndex % spinFrames.length];
            sb.append(frame);
            lastFrameIndex++;
        }
        int currentContentLen = sb.length() - 1;
        int remainingCount = width - currentContentLen;
        sb.repeat(String.valueOf(remainingChar), Math.max(0, remainingCount));
        sb.append(']');
        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("%s %s (%s) - %s - ETA: %s",
                getProgressBar(),
                formatPercent(),
                getProgressStatus(),
                formatSpeed(getInstantSpeed()),
                formatDuration(getEstimateArrivalTime())
        );
    }

    /** 格式化字节大小 */
    public static String formatSize(long bytes) {
        if (bytes <= 0) {
            return "0.00 B";
        }
        int digitGroup = (int) (Math.log(bytes) / Math.log(1024));
        digitGroup = Math.min(digitGroup, SIZE_UNITS.length - 1);
        double value = bytes / Math.pow(1024, digitGroup);
        return String.format("%.2f %s", value, SIZE_UNITS[digitGroup]);
    }

    /** 格式化字节速度 */
    public static String formatSpeed(double bytesPerSecond) {
        if (bytesPerSecond <= 0) {
            return "0.00 B/s";
        }
        int digitGroup = (int) (Math.log(bytesPerSecond) / Math.log(1024));
        digitGroup = Math.min(digitGroup, SPEED_UNITS.length - 1);
        double value = bytesPerSecond / Math.pow(1024, digitGroup);
        return String.format("%.2f %s", value, SPEED_UNITS[digitGroup]);
    }

    /** 格式化剩余时间 (HH:mm:ss) */
    public static String formatDuration(long seconds) {
        if (seconds < 0) {
            return "--:--:--";
        }
        long h = seconds / 3600;
        long m = (seconds % 3600) / 60;
        long s = seconds % 60;
        return String.format("%02d:%02d:%02d", h, m, s);
    }
}
