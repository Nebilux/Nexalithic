package com.thezeroer.nexalithic.core.messaging.task.visual;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 传输快照
 * 核心原理：利用 VarHandle 的 Release/Acquire 语义，实现 IO 线程写开销最小化，
 * 同时保证监控线程能读到一致的进度数据。
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/06
 * @version 1.0.1
 */
public class TransferSnapshot {
    private static final String[] SIZE_UNITS = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    private static final String[] SPEED_UNITS = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s", "PB/s", "EB/s"};
    private static final char[] SPIN_FRAMES = {'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'};

    private static final VarHandle VH;
    static {
        try {
            VH = MethodHandles.lookup().findVarHandle(TransferSnapshot.class, "remaining", long.class);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private final long total;
    private long remaining;

    private final long startTimeNanos;
    private long lastSnapshotTimeNanos;
    private long lastRemaining;
    private int lastFrameIndex;

    public TransferSnapshot(long total) {
        this.total = total;
        this.startTimeNanos = System.nanoTime();
        this.lastSnapshotTimeNanos = startTimeNanos;
        this.lastRemaining = total;
        updateRemaining(total);
    }

    public void updateRemaining(long remaining) {
        VH.setRelease(this, remaining);
    }

    public long getTotal() {
        return total;
    }
    public long getRemaining() {
        return (long) VH.getAcquire(this);
    }
    public long getProcessed() {
        return total - getRemaining();
    }

    /**
     * 计算自开始以来的平均速度 (Bytes/s)
     */
    public double getAverageSpeed() {
        long durationNanos = System.nanoTime() - startTimeNanos;
        if (durationNanos <= 0) {
            return 0;
        }
        return (getProcessed() * 1_000_000_000.0) / durationNanos;
    }

    /**
     * 计算瞬时速度 (最近两次轮询间的速度)
     */
    public double getInstantSpeed() {
        long currentTimeNanos = System.nanoTime();
        long currentRemaining = getRemaining();
        long timeDelta = currentTimeNanos - lastSnapshotTimeNanos;
        if (timeDelta <= 0) {
            return 0;
        }
        long bytesDelta = lastRemaining - currentRemaining;
        double speed = (bytesDelta * 1_000_000_000.0) / timeDelta;
        this.lastSnapshotTimeNanos = currentTimeNanos;
        this.lastRemaining = currentRemaining;
        return Math.max(0, speed);
    }

    /**
     * 预计剩余时间 (秒)
     * 基于瞬时速度计算，如果瞬时速度为0则回退到平均速度
     */
    public long getEstimateArrivalTime() {
        double speed = getInstantSpeed();
        if (speed <= 0) {
            speed = getAverageSpeed();
        }
        if (speed <= 0) {
            return -1;
        }
        return (long) (getRemaining() / speed);
    }

    /**
     * 获取当前进度百分比 (0.00 - 100.00)
     */
    public double getPercent() {
        if (total <= 0) return 100.0;
        // 使用同步读取的 processed 值
        double percent = (getProcessed() * 100.0) / total;
        return Math.min(100.0, Math.max(0.0, percent));
    }

    /**
     * 格式化当前进度百分比字符串
     */
    public String formatPercent() {
        return String.format("%.2f%%", getPercent());
    }

    /**
     * 获取格式化的进度描述 (e.g., "1.24 MB / 10.00 MB")
     * */
    public String getProgressStatus() {
        return formatSize(getProcessed()) + " / " + formatSize(total);
    }

    /**
     * 生成默认长度（100格）的文本进度条
     * 示例: [##########----------]
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
        long processed = getProcessed();
        int completedWidth = (int) (width * (percent / 100.0));
        completedWidth = Math.min(width, Math.max(0, completedWidth));
        StringBuilder sb = new StringBuilder(width + 2);
        sb.append('[');
        boolean showSpin = spinFrames != null && spinFrames.length > 0 && processed > 0 && percent < 100.0;
        int fillWidth = showSpin ? Math.min(completedWidth, width - 1) : completedWidth;
        sb.append(String.valueOf(completedChar).repeat(Math.max(0, fillWidth)));
        if (showSpin) {
            char frame = spinFrames[lastFrameIndex % spinFrames.length];
            sb.append(frame);
            lastFrameIndex++;
        }
        int currentContentLen = sb.length() - 1;
        int remainingCount = width - currentContentLen;
        sb.append(String.valueOf(remainingChar).repeat(Math.max(0, remainingCount)));
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
