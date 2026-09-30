package com.nebilux.nexalithic.core.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 时间利用
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/15
 * @version 1.0.0
 */
public class TimeUtils {
    private static final Map<String, DateTimeFormatter> FORMATTER_CACHE = new ConcurrentHashMap<>();
    private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    public static String format(long timestamp) {
        return DEFAULT_FORMATTER.format(Instant.ofEpochMilli(timestamp));
    }

    public static String formatCustom(long timestamp, String pattern) {
        DateTimeFormatter df = FORMATTER_CACHE.computeIfAbsent(pattern, p -> 
            DateTimeFormatter.ofPattern(p).withZone(ZoneId.systemDefault())
        );
        return df.format(Instant.ofEpochMilli(timestamp));
    }
}