package com.nebilux.nexalithic.core.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 二进制存储工具类
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/12
 * @version 1.0.0
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