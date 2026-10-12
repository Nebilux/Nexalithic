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

package com.nebilux.nexalithic.core.io.codec.assembler;

import com.nebilux.nexalithic.core.io.codec.TaskCodecCallback;
import com.nebilux.nexalithic.core.messaging.task.TaskScheduler;
import com.nebilux.nexalithic.core.messaging.task.visual.TransferListener;
import com.nebilux.nexalithic.core.model.packet.business.BusinessPacket;

/**
 * 装配回调
 *
 * @author Reonvia
 * @since 0.2.0
 */
public class AssemblyCallback extends TaskCodecCallback {
    public AssemblyCallback(TaskScheduler scheduler) {
        super(scheduler);
    }

    @Override
    protected boolean accept(BusinessPacket.Way way) {
        return way.isResponse();
    }

    @Override
    protected TransferListener transferListener() {
        return task.getResponseListener();
    }
}
