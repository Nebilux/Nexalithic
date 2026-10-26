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

package com.nebilux.nexalithic.core.security;

import com.nebilux.nexalithic.core.connection.channel.NexalithicChannel;

import javax.crypto.NoSuchPaddingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.EnumMap;
import java.util.Objects;

/**
 * 密钥上下文集
 *
 * @author Reonvia
 * @since 0.5.0
 */
public class SecretKeyContexts {
    private static final byte[] LABEL_CLIENT_SIGNALING_PACKET = "LABEL_CLIENT_SIGNALING_PACKET".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LABEL_SERVER_SIGNALING_PACKET = "LABEL_SERVER_SIGNALING_PACKET".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LABEL_CLIENT_BUSINESS_PACKET = "LABEL_CLIENT_BUSINESS_PACKET".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LABEL_SERVER_BUSINESS_PACKET = "LABEL_SERVER_BUSINESS_PACKET".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LABEL_CLIENT_BUSINESS_Stream = "LABEL_CLIENT_BUSINESS_Stream".getBytes(StandardCharsets.UTF_8);
    private static final byte[] LABEL_SERVER_BUSINESS_Stream = "LABEL_SERVER_BUSINESS_Stream".getBytes(StandardCharsets.UTF_8);

    public enum Side {
        CLIENT, SERVER;

        Side peer() {
            return this == CLIENT ? SERVER : CLIENT;
        }
    }
    private static final NexalithicChannel.Kind[] KINDS = NexalithicChannel.Kind.values();

    private final EnumMap<NexalithicChannel.Kind, SecretKeyContext> contexts;

    private SecretKeyContexts(EnumMap<NexalithicChannel.Kind, SecretKeyContext> contexts) {
        this.contexts = contexts;
    }

    public static SecretKeyContexts fromSecret(byte[] secret, Side side) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        EnumMap<NexalithicChannel.Kind, SecretKeyContext> contexts = new EnumMap<>(NexalithicChannel.Kind.class);
        for (NexalithicChannel.Kind kind : KINDS) {
            contexts.put(kind, SecretKeyUtils.generateSessionSecretKey(
                    secret,
                    label(kind, side),        // 本端发送、对端接收
                    label(kind, side.peer())  // 对端发送、本端接收
            ));
        }
        return new SecretKeyContexts(contexts);
    }

    /** 返回该通道实际持有的上下文；握手已消耗的信令序号由同一实例继续维护。 */
    public SecretKeyContext get(NexalithicChannel.Kind kind) {
        return Objects.requireNonNull(contexts.get(kind), "Missing key for " + kind);
    }

    private static byte[] label(NexalithicChannel.Kind kind, Side side) {
        return switch (kind) {
            case Signaling_Packet -> side == Side.CLIENT
                    ? LABEL_CLIENT_SIGNALING_PACKET
                    : LABEL_SERVER_SIGNALING_PACKET;
            case Business_Packet -> side == Side.CLIENT
                    ? LABEL_CLIENT_BUSINESS_PACKET
                    : LABEL_SERVER_BUSINESS_PACKET;
            case Business_Stream -> side == Side.CLIENT
                    ? LABEL_CLIENT_BUSINESS_Stream
                    : LABEL_SERVER_BUSINESS_Stream;
        };
    }
}
