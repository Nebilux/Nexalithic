package com.nebilux.nexalithic.core.builder.option;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;
import com.nebilux.nexalithic.core.util.ClassScanner;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 选项集定义
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/02
 * @version 1.0.0
 */
public abstract class OptionsDefinition {
    protected final Class<?> holder;

    protected OptionsDefinition(Class<?> holder) {
        this.holder = Objects.requireNonNull(holder, "holder");
    }

    protected <T> NexalithicOption<T> defineOption(T defaultValue, OptionValidator<T> validator) {
        return new NexalithicOption<>(defaultValue, null, validator);
    }
    protected <T> NexalithicOption<T> defineOptionLazy(Function<NexalithicBuilderContext, T> defaultValueLazy, OptionValidator<T> validator) {
        return new NexalithicOption<>(null, defaultValueLazy, validator);
    }

    public static <T extends OptionsDefinition> T initOptions(Class<T> options, Class<?> holder) {
        try {
            Constructor<T> constructor = options.getDeclaredConstructor(Class.class);
            constructor.setAccessible(true);
            T instance = constructor.newInstance(holder);
            instance.applyNamespacing(holder.getSimpleName());
            return instance;
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to auto-namespace options", e);
        }
    }
    void applyNamespacing(String prefix) throws IllegalAccessException {
        Class<?> base = this.getClass();
        Class<?> current = base;
        while (current != null && current != OptionsDefinition.class) {
            Field[] fields = current.getDeclaredFields();
            String currentPrefix = (current == base) ? prefix : prefix + "(" + current.getEnclosingClass().getSimpleName() + ")";
            for (Field field : fields) {
                if (field.isSynthetic() || field.getName().startsWith("this$")) {
                    continue;
                }
                field.setAccessible(true);
                Object value = field.get(this);
                switch (value) {
                    case NexalithicOption<?> option -> {
                        if (option.name() == null) {
                            option.setName(currentPrefix + "_" + field.getName());
                        }
                    }
                    case OptionsDefinition subOptions -> subOptions.applyNamespacing(prefix + "_" + field.getName());
                    case null, default -> {}
                }
            }
            current = current.getSuperclass();
        }
    }

    @Override
    public String toString() {
        return toString(null);
    }
    public String toString(NexalithicBuilderContext context) {
        StringBuilder sb = new StringBuilder();
        sb.append(holder.getSimpleName()).append("-").append(this.getClass().getSimpleName()).append(" {\n");
        buildString(sb, 1, context);
        sb.append("}");
        return sb.toString();
    }
    public static String toString(String packageName, NexalithicBuilderContext context) {
        StringBuilder sb = new StringBuilder();
        List<Class<?>> classes = ClassScanner.scan(packageName);
        for (Class<?> clazz : classes) {
            if (clazz.isSynthetic() || clazz.isAnonymousClass()) {
                continue;
            }
            try {
                Field optionsField = clazz.getDeclaredField("OPTIONS");
                if (!Modifier.isStatic(optionsField.getModifiers())) {
                    continue;
                }
                if (!OptionsDefinition.class.isAssignableFrom(optionsField.getType())) {
                    continue;
                }
                optionsField.setAccessible(true);
                if (optionsField.get(null) instanceof OptionsDefinition options) {
                    if (Modifier.isFinal(options.getClass().getModifiers())) {
                        sb.append(options.toString(context)).append("\n");
                    }
                }
            } catch (NoSuchFieldException | IllegalAccessException ignored) {}
        }
        return sb.toString();
    }

    private void buildString(StringBuilder sb, int indent, NexalithicBuilderContext context) {
        String prefix = "  ".repeat(indent);
        Class<?> clazz = this.getClass();
        int maxLabelLen = 0;
        int maxValueLen = 0;
        Class<?> scanClazz = clazz;
        while (scanClazz != null && scanClazz != OptionsDefinition.class) {
            for (Field field : scanClazz.getDeclaredFields()) {
                if (field.isSynthetic() || field.getName().startsWith("this$")) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    Object val = field.get(this);
                    if (val instanceof NexalithicOption<?> opt) {
                        maxLabelLen = Math.max(maxLabelLen, field.getName().length());
                        maxValueLen = Math.max(maxValueLen, getValueString(opt, context).length());
                    } else if (val instanceof OptionsDefinition) {
                        maxLabelLen = Math.max(maxLabelLen, field.getName().length());
                    }
                } catch (IllegalAccessException ignored) {}
            }
            scanClazz = scanClazz.getSuperclass();
        }
        int labelTarget = maxLabelLen + 3;
        int valueTarget = maxValueLen + 3;
        while (clazz != null && clazz != OptionsDefinition.class) {
            for (Field field : clazz.getDeclaredFields()) {
                if (field.isSynthetic() || field.getName().startsWith("this$")) {
                    continue;
                }
                try {
                    field.setAccessible(true);
                    Object value = field.get(this);
                    String displayName = field.getName();
                    if (value instanceof NexalithicOption<?> option) {
                        sb.append(prefix).append(displayName);
                        int dots1 = labelTarget - displayName.length();
                        sb.append(".".repeat(Math.max(1, dots1)));
                        String valueStr = getValueString(option, context);
                        sb.append(valueStr);
                        int dots2 = valueTarget - valueStr.length();
                        sb.append(".".repeat(Math.max(1, dots2)));
                        sb.append("[").append(option.name()).append("]\n");
                    } else if (value instanceof OptionsDefinition subOptions) {
                        sb.append(prefix).append(displayName).append(": {\n");
                        subOptions.buildString(sb, indent + 1, context);
                        sb.append(prefix).append("}\n");
                    }
                } catch (IllegalAccessException ignored) {}
            }
            clazz = clazz.getSuperclass();
        }
    }
    private static String getValueString(NexalithicOption<?> opt, NexalithicBuilderContext context) {
        StringBuilder vsb = new StringBuilder();
        vsb.append("(default=").append(opt.defaultValue(context));
        if (context != null) {
            vsb.append(", current=").append(context.getOption(opt));
        }
        vsb.append(")");
        return vsb.toString();
    }
}
