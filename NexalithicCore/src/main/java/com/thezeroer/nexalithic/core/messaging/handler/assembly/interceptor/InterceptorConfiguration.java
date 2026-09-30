package com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor;

import java.util.Map;

/**
 * 拦截器配置。
 *
 * <p>配置对象保存注解解析后的通用键值参数。所有属性值以字符串形式存储，
 * 拦截器创建器可以通过本类提供的类型化读取方法取得所需参数。</p>
 *
 * <p>构造后配置内容不可变。无默认值的读取方法在属性缺失时抛出
 * {@link IllegalArgumentException}；数值转换失败时也会抛出该异常并保留原始原因。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/12
 */
public class InterceptorConfiguration {
    private final String name;
    private final Map<String, String> properties;

    /**
     * 创建拦截器配置。
     *
     * @param name 配置名称，通常用于标识来源注解或配置项
     * @param properties 解析后的属性映射
     */
    public InterceptorConfiguration(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = Map.copyOf(properties);
    }

    /**
     * 返回配置名称。
     *
     * @return 配置名称
     */
    public String getName() {
        return name;
    }

    /**
     * 读取字符串属性。
     *
     * @param propertyName 属性名
     * @return 属性值
     * @throws IllegalArgumentException 属性不存在时抛出
     */
    public String getStringValue(String propertyName) {
        String value = properties.get(propertyName);
        if (value == null) {
            throw new IllegalArgumentException("Missing interceptor property: " + propertyName);
        }
        return value;
    }

    /**
     * 读取字符串属性，属性不存在时返回默认值。
     *
     * @param propertyName 属性名
     * @param defaultValue 默认值
     * @return 属性值或默认值
     */
    public String getStringValue(String propertyName, String defaultValue) {
        String value = properties.get(propertyName);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    /**
     * 读取 {@code int} 属性。
     *
     * @param propertyName 属性名
     * @return 转换后的整数值
     * @throws IllegalArgumentException 属性不存在或无法解析为 {@code int} 时抛出
     */
    public int getIntValue(String propertyName) {
        String value = getStringValue(propertyName);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid int interceptor property: " + propertyName + "=" + value, e);
        }
    }

    /**
     * 读取 {@code int} 属性，属性不存在时返回默认值。
     *
     * @param propertyName 属性名
     * @param defaultValue 默认值
     * @return 转换后的整数值或默认值
     * @throws IllegalArgumentException 属性存在但无法解析为 {@code int} 时抛出
     */
    public int getIntValue(String propertyName, int defaultValue) {
        String value = properties.get(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid int interceptor property: " + propertyName + "=" + value, e);
        }
    }

    /**
     * 读取 {@code long} 属性。
     *
     * @param propertyName 属性名
     * @return 转换后的长整数值
     * @throws IllegalArgumentException 属性不存在或无法解析为 {@code long} 时抛出
     */
    public long getLongValue(String propertyName) {
        String value = getStringValue(propertyName);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long interceptor property: " + propertyName + "=" + value, e);
        }
    }

    /**
     * 读取 {@code long} 属性，属性不存在时返回默认值。
     *
     * @param propertyName 属性名
     * @param defaultValue 默认值
     * @return 转换后的长整数值或默认值
     * @throws IllegalArgumentException 属性存在但无法解析为 {@code long} 时抛出
     */
    public long getLongValue(String propertyName, long defaultValue) {
        String value = properties.get(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long interceptor property: " + propertyName + "=" + value, e);
        }
    }

    /**
     * 读取 {@code float} 属性。
     *
     * @param propertyName 属性名
     * @return 转换后的浮点值
     * @throws IllegalArgumentException 属性不存在或无法解析为 {@code float} 时抛出
     */
    public float getFloatValue(String propertyName) {
        String value = getStringValue(propertyName);
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid float interceptor property: " + propertyName + "=" + value, e);
        }
    }

    /**
     * 读取 {@code float} 属性，属性不存在时返回默认值。
     *
     * @param propertyName 属性名
     * @param defaultValue 默认值
     * @return 转换后的浮点值或默认值
     * @throws IllegalArgumentException 属性存在但无法解析为 {@code float} 时抛出
     */
    public float getFloatValue(String propertyName, float defaultValue) {
        String value = properties.get(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid float interceptor property: " + propertyName + "=" + value, e);
        }
    }

    /**
     * 读取 {@code double} 属性。
     *
     * @param propertyName 属性名
     * @return 转换后的双精度浮点值
     * @throws IllegalArgumentException 属性不存在或无法解析为 {@code double} 时抛出
     */
    public double getDoubleValue(String propertyName) {
        String value = getStringValue(propertyName);
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid double interceptor property: " + propertyName + "=" + value, e);
        }
    }

    /**
     * 读取 {@code double} 属性，属性不存在时返回默认值。
     *
     * @param propertyName 属性名
     * @param defaultValue 默认值
     * @return 转换后的双精度浮点值或默认值
     * @throws IllegalArgumentException 属性存在但无法解析为 {@code double} 时抛出
     */
    public double getDoubleValue(String propertyName, double defaultValue) {
        String value = properties.get(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid double interceptor property: " + propertyName + "=" + value, e);
        }
    }

    /**
     * 读取 {@code boolean} 属性。
     *
     * <p>布尔值使用 {@link Boolean#parseBoolean(String)} 解析。</p>
     *
     * @param propertyName 属性名
     * @return 转换后的布尔值
     * @throws IllegalArgumentException 属性不存在时抛出
     */
    public boolean getBooleanValue(String propertyName) {
        return Boolean.parseBoolean(getStringValue(propertyName));
    }

    /**
     * 读取 {@code boolean} 属性，属性不存在时返回默认值。
     *
     * <p>布尔值使用 {@link Boolean#parseBoolean(String)} 解析。</p>
     *
     * @param propertyName 属性名
     * @param defaultValue 默认值
     * @return 转换后的布尔值或默认值
     */
    public boolean getBooleanValue(String propertyName, boolean defaultValue) {
        String value = properties.get(propertyName);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }
}
