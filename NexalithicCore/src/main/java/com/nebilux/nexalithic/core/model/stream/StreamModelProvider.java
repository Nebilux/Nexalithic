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

package com.nebilux.nexalithic.core.model.stream;

import com.nebilux.nexalithic.core.internal.model.stream.StreamModelProviderImpl;
import com.nebilux.nexalithic.core.model.stream.file.FileStream;
import com.nebilux.nexalithic.core.model.stream.media.MediaStream;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * 创建文件流和媒体流的提供程序。首次调用 {@link #provider()} 时选择一次默认实例：
 * 系统类加载器可见的服务实现优先，否则使用框架内置实现。
 * 自定义提供程序须为两个流类型提供与其编解码器兼容的实现。
 *
 * @author Reonvia
 * @since 0.5.0
 */
public abstract class StreamModelProvider {
    private static final class Holder {
        static final StreamModelProvider INSTANCE = loadProvider();

        private static StreamModelProvider loadProvider() {
            StreamModelProvider selected = loadFromService();
            return selected != null ? selected : StreamModelProviderImpl.INSTANCE;
        }

        private static StreamModelProvider loadFromService() {
            Iterator<StreamModelProvider> providers = ServiceLoader.load(StreamModelProvider.class, ClassLoader.getSystemClassLoader()).iterator();
            return providers.hasNext() ? providers.next() : null;
        }
    }

    protected StreamModelProvider() {}

    /** 返回 JVM 中选定的默认提供程序。首次选择后不会因服务变化而替换。 */
    public static StreamModelProvider provider() {
        return Holder.INSTANCE;
    }

    /** 创建指定方向的媒体流；方向以应用为参照。 */
    public abstract MediaStream openMediaStream(AbstractStream.Direction direction);

    /** 创建指定方向的文件流；方向以应用为参照。 */
    public abstract FileStream openFileStream(AbstractStream.Direction direction);
}
