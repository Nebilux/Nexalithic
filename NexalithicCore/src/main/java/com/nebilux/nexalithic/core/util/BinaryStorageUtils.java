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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 二进制存储工具类
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BinaryStorageUtils {

    /**
     * 将 byte[] 直接写入文件
     */
    public static void saveBytes(byte[] data, String filePath) throws Exception {
        Path path = Paths.get(filePath);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.write(path, data);
    }

    /**
     * 从文件加载 byte[]
     */
    public static byte[] loadBytes(String filePath) throws IOException {
        return Files.readAllBytes(Paths.get(filePath));
    }
}