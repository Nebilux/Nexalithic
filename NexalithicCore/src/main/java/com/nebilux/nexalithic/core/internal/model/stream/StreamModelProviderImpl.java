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

package com.nebilux.nexalithic.core.internal.model.stream;

import com.nebilux.nexalithic.core.model.stream.AbstractStream;
import com.nebilux.nexalithic.core.model.stream.StreamModelProvider;
import com.nebilux.nexalithic.core.model.stream.file.FileStream;
import com.nebilux.nexalithic.core.model.stream.media.MediaStream;

/**
 * 流模型提供者impl
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class StreamModelProviderImpl extends StreamModelProvider {
    public static StreamModelProviderImpl INSTANCE = new StreamModelProviderImpl();

    protected StreamModelProviderImpl() {}

    @Override
    public MediaStream openMediaStream(AbstractStream.Direction direction) {
        return new MediaStreamImpl(direction);
    }

    @Override
    public FileStream openFileStream(AbstractStream.Direction direction) {
        return new FileStreamImpl(direction);
    }
}
