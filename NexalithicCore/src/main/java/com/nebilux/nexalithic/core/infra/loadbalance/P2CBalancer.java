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

package com.nebilux.nexalithic.core.infra.loadbalance;

import java.util.concurrent.ThreadLocalRandom;

/**
 * P2平衡器,“Power of Two Choices”（随机选两个，取较优者）
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class P2CBalancer<T extends LoadBalanceable> implements LoadBalancer<Void, T> {
    private final T[] candidates;

    public P2CBalancer(T[] candidates) {
        if (candidates == null || candidates.length == 0) {
            throw new IllegalArgumentException("Candidates cannot be empty");
        }
        this.candidates = candidates;
    }

    @Override
    public T select(Void ignored) {
        int length = candidates.length;
        if (length == 1) {
            return candidates[0];
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int idx1 = random.nextInt(length);
        int idx2 = random.nextInt(length);
        if (idx1 == idx2) {
            idx2 = (idx1 + 1) % length;
        }
        T c1 = candidates[idx1];
        T c2 = candidates[idx2];
        return (c1.getLoadScore() <= c2.getLoadScore()) ? c1 : c2;
    }

    @Override
    public T[] all() {
        return candidates;
    }
}