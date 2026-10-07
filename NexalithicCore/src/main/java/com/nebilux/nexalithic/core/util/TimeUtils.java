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

package com.nebilux.nexalithic.core.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 时间利用
 *
 * @author Reonvia
 * @since 0.1.0
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