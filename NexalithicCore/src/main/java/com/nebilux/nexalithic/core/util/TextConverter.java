package com.nebilux.nexalithic.core.util;

import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 文本数据转换器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class TextConverter {
    private static final Pattern LIST_PATTERN = Pattern.compile("\"((?:[^\"\\\\]|\\\\.)*)\"");
    private static final Pattern MAP_PATTERN = Pattern.compile("\"((?:[^\"\\\\]|\\\\.)*)\":\"((?:[^\"\\\\]|\\\\.)*)\"");

    /**
     * 将任何 String 集合序列化为 NTP 格式: ["a","b"]
     */
    public static String fromCollection(Collection<String> collection) {
        if (collection == null || collection.isEmpty()) {
            return "[]";
        }
        StringBuilder result = new StringBuilder("[");
        Iterator<String> it = collection.iterator();
        while (it.hasNext()) {
            result.append("\"").append(escape(it.next())).append("\"");
            if (it.hasNext()) {
                result.append(",");
            }
        }
        return result.append("]").toString();
    }

    /**
     * 将字符串解析并填充到指定的集合容器中
     * @param text      待解析字符串
     * @param supplier  集合工厂，例如 ArrayList::new 或 LinkedHashSet::new
     */
    public static <T extends Collection<String>> T toCollection(String text, Supplier<T> supplier) {
        T result = supplier.get();
        if (text == null || text.length() < 2) {
            return result;
        }
        Matcher matcher = LIST_PATTERN.matcher(text);
        while (matcher.find()) {
            result.add(unescape(matcher.group(1)));
        }
        return result;
    }

    public static String fromList(List<String> list) {
        return fromCollection(list);
    }

    public static String fromSet(Set<String> set) {
        return fromCollection(set);
    }

    public static List<String> toList(String text) {
        return toCollection(text, ArrayList::new);
    }

    public static Set<String> toSet(String text) {
        return toCollection(text, LinkedHashSet::new);
    }

    public static String fromArray(String... array) {
        if (array == null || array.length == 0) {
            return "[]";
        }
        return fromCollection(Arrays.asList(array));
    }

    public static String[] toArray(String text) {
        if (text == null || text.length() < 2) {
            return new String[0];
        }
        return toList(text).toArray(new String[0]);
    }

    public static String fromListArray(List<String[]> listArray) {
        if (listArray == null || listArray.isEmpty()) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < listArray.size(); i++) {
            result.append(fromCollection(Arrays.asList(listArray.get(i))));
            if (i < listArray.size() - 1) {
                result.append(";");
            }
        }
        return result.toString();
    }

    public static List<String[]> toListArray(String text) {
        List<String[]> result = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return result;
        }
        List<String> rows = splitByUnescapedSemicolon(text);
        for (String row : rows) {
            result.add(toList(row).toArray(new String[0]));
        }
        return result;
    }

    public static String fromMap(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        StringBuilder result = new StringBuilder("{");
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            result.append("\"").append(escape(entry.getKey())).append("\":")
                    .append("\"").append(escape(entry.getValue())).append("\"");
            if (it.hasNext()) {
                result.append(",");
            }
        }
        return result.append("}").toString();
    }

    public static <T extends Map<String, String>> T toMap(String text, Supplier<T> supplier) {
        T result = supplier.get();
        if (text == null || text.length() < 2) {
            return result;
        }
        Matcher matcher = MAP_PATTERN.matcher(text);
        while (matcher.find()) {
            result.put(unescape(matcher.group(1)), unescape(matcher.group(2)));
        }
        return result;
    }

    public static Map<String, String> toMap(String text) {
        return toMap(text, HashMap::new);
    }

    private static String escape(String s) {
        if (s == null) {
            return "";
        }
        // 先处理反斜杠，再处理引号和分号
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace(";", "\\;");
    }

    private static String unescape(String s) {
        if (s == null || s.indexOf('\\') == -1) {
            return s; // 快速路径：没有反斜杠直接返回原字符串，避免创建 StringBuilder
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(i + 1);
                // 只处理定义的转义集
                if (next == '"' || next == '\\' || next == ';') {
                    sb.append(next);
                    i++;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static List<String> splitByUnescapedSemicolon(String str) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (escaped) {
                // 如果前一个字符是 \，无论当前是什么都视为字面量
                current.append(c);
                escaped = false;
            } else if (c == '\\') {
                // 标记进入转义状态，且必须把 \ 存入，
                // 因为 stringToList 还需要它来识别引号转义
                current.append(c);
                escaped = true;
            } else if (c == ';') {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) {
            parts.add(current.toString());
        }
        return parts;
    }
}