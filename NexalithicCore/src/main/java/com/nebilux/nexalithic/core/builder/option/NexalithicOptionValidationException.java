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

import com.nebilux.nexalithic.core.exception.NexalithicUncheckedException;

/**
 * 终端配置选项未通过校验的异常。
 *
 * <p>通常在设置选项时，选项值不满足该选项的校验规则；
 * 异常消息包含选项名和校验失败的具体原因。</p>
 *
 * @author Reonvia
 * @since 0.1.0
 */
public final class NexalithicOptionValidationException extends NexalithicUncheckedException {
    private NexalithicOptionValidationException(String message) {
        super(message, true);
    }

    private NexalithicOptionValidationException(String message, Throwable cause) {
        super(message, cause, false, true);
    }

    public static NexalithicOptionValidationException forOption(String optionName, String detail) {
        return new NexalithicOptionValidationException(messageFor(optionName, detail));
    }

    public static NexalithicOptionValidationException forOption(String optionName, String detail, Throwable cause) {
        return new NexalithicOptionValidationException(messageFor(optionName, detail), cause);
    }

    private static String messageFor(String optionName, String detail) {
        return "Option error at [" + optionName + "]: " + detail;
    }
}
