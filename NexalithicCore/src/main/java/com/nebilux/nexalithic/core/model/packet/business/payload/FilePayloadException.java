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

package com.nebilux.nexalithic.core.model.packet.business.payload;

import com.nebilux.nexalithic.core.exception.NexalithicCheckedException;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * 创建文件载荷时找不到源文件的受检异常。
 *
 * <p>传入的源文件在构造时不存在便会产生；文件路径写入异常消息，
 * 底层 {@link FileNotFoundException} 可通过 {@link #getCause()} 获取。</p>
 */
public final class FilePayloadException extends NexalithicCheckedException {
    private FilePayloadException(String message, Throwable cause) {
        super(message, cause);
    }

    public static FilePayloadException missingSource(File file) {
        String path = file.getAbsolutePath();
        return new FilePayloadException("Source file not found: " + path, new FileNotFoundException(path));
    }
}
