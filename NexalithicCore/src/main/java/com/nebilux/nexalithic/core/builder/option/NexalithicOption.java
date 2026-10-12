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

package com.nebilux.nexalithic.core.builder.option;

import com.nebilux.nexalithic.core.builder.NexalithicBuilderContext;

import java.util.function.Function;

/**
 * Nexalithic选项
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class NexalithicOption<T> {
    private volatile String name;
    private final T defaultValue;
    private final Function<NexalithicBuilderContext, T> defaultValueLazy;
    private final OptionValidator<T> validator;

    NexalithicOption(T defaultValue, Function<NexalithicBuilderContext, T> defaultValueLazy, OptionValidator<T> validator) {
        if (validator != null && defaultValueLazy == null) {
            validator.validate(defaultValue);
        }
        this.defaultValue = defaultValue;
        this.defaultValueLazy = defaultValueLazy;
        this.validator = validator;
    }
    void setName(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }
    public T defaultValue() {
        return defaultValue;
    }
    public T defaultValue(NexalithicBuilderContext context) {
        T value = defaultValue;
        if (defaultValueLazy != null) {
            value = defaultValueLazy.apply(context);
            validate(value);
        }
        return value;
    }
    public NexalithicOption<T> validate(T value) {
        if (validator != null) {
            try {
                validator.validate(value);
            } catch (IllegalArgumentException e) {
                throw NexalithicOptionValidationException.forOption(name, e.getMessage(), e);
            }
        }
        return this;
    }

    @Override
    public String toString() {
        if (defaultValueLazy == null) {
            return String.format("%s(default=%s)", name, defaultValue);
        } else {
            return String.format("%s(default=%s)", name, defaultValueLazy);
        }
    }
}
