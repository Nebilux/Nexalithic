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

package com.nebilux.nexalithic.core.connection.channel;

import com.nebilux.nexalithic.core.connection.session.AbstractSession;
import com.nebilux.nexalithic.core.io.codec.assembler.ModelAssembler;
import com.nebilux.nexalithic.core.io.codec.fragmenter.ModelFragmenter;
import com.nebilux.nexalithic.core.io.loop.SessionLoop;
import com.nebilux.nexalithic.core.model.business.BusinessModel;
import com.nebilux.nexalithic.core.security.SecretKeyContext;

/**
 * 会议业务频道
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class SessionBusinessChannel<M extends BusinessModel, S extends AbstractSession<S>> extends SessionChannel<M ,S> {
    public SessionBusinessChannel(Kind kind, SessionLoop<?, ? super SessionChannel<M, S>> ownerLoop, S ownerSession,
                                  ModelFragmenter<M> fragmenter, ModelAssembler<M> assembler, SecretKeyContext secretKeyContext) {
        super(kind, ownerLoop, ownerSession, fragmenter, assembler, secretKeyContext);
    }
}
