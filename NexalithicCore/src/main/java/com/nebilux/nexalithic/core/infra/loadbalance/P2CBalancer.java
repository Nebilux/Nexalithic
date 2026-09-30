package com.nebilux.nexalithic.core.infra.loadbalance;

import java.util.concurrent.ThreadLocalRandom;

/**
 * P2平衡器,“Power of Two Choices”（随机选两个，取较优者）
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/02/21
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