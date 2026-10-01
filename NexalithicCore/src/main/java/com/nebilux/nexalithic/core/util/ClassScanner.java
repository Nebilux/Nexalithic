package com.nebilux.nexalithic.core.util;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 类扫描器
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class ClassScanner {

    public static List<Class<?>> scan(String packageName) {
        List<Class<?>> classes = new ArrayList<>();
        // 将包名转换为路径名：com.nexalithic -> com/nexalithic
        String packagePath = packageName.replace('.', '/');
        
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(packagePath);
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                String protocol = url.getProtocol();

                if ("file".equals(protocol)) {
                    // 处理本地文件系统中的 .class 文件
                    String filePath = URLDecoder.decode(url.getFile(), StandardCharsets.UTF_8);
                    findClassesInDirectory(packageName, filePath, classes);
                } else if ("jar".equals(protocol)) {
                    // 处理 JAR 包中的 .class 文件
                    findClassesInJar(url, packagePath, classes);
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to scan package: " + packageName, e);
        }
        return classes;
    }

    /**
     * 在文件夹中递归寻找 .class 文件
     */
    private static void findClassesInDirectory(String packageName, String path, List<Class<?>> classes) throws ClassNotFoundException {
        File dir = new File(path);
        if (!dir.exists() || !dir.isDirectory()) return;

        File[] files = dir.listFiles(f -> f.isDirectory() || f.getName().endsWith(".class"));
        if (files == null) return;

        for (File file : files) {
            if (file.isDirectory()) {
                // 递归子目录
                findClassesInDirectory(packageName + "." + file.getName(), file.getAbsolutePath(), classes);
            } else {
                // 加载类
                String className = packageName + "." + file.getName().substring(0, file.getName().length() - 6);
                classes.add(Class.forName(className));
            }
        }
    }

    /**
     * 在 JAR 包中寻找指定路径下的 .class 文件
     */
    private static void findClassesInJar(URL url, String packagePath, List<Class<?>> classes) throws IOException, ClassNotFoundException {
        JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
        try (JarFile jarFile = jarURLConnection.getJarFile()) {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String name = entry.getName();
                if (name.startsWith(packagePath) && name.endsWith(".class")) {
                    // 将路径名转回包名并加载类
                    String className = name.replace('/', '.').substring(0, name.length() - 6);
                    classes.add(Class.forName(className));
                }
            }
        }
    }
}