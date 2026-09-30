package com.thezeroer.nexalithic.core.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 生命周期管理器
 *
 * @author tbrtz647@outlook.com
 * @since 2026/04/14
 * @version 1.0.0
 */
public abstract class LifecycleManager {
    /**
     * Nexalithic终端的生命周期状态枚举。
     * <p>定义了终端从创建到终止的完整状态转换过程，用于控制终端的生命周期管理。</p>
     * <p><b>状态转换流程：</b><br>
     * {@link #NEW} → {@link #STARTING} → {@link #RUNNING} → ({@link #STOPPING} 或 {@link #SHUTTING_DOWN}) → {@link #TERMINATED}<br>
     * 任何状态都可能直接转换为 {@link #ERROR}（发生异常时）</p>
     */
    public enum State {
        /**
         * 终端的初始状态。
         * <p>终端刚创建但尚未调用{@link #start()}方法时的状态。</p>
         */
        NEW,
        /**
         * 终端正在启动中的状态。
         * <p>调用{@link #start()}方法后，终端开始启动各个组件时的状态。</p>
         */
        STARTING,
        /**
         * 终端正常运行的状态。
         * <p>所有核心组件都已成功启动，终端能够正常处理请求时的状态。</p>
         */
        RUNNING,
        /**
         * 终端正在停止中的状态。
         * <p>调用{@link #stop()}方法后，终端开始停止各个组件时的状态。</p>
         */
        STOPPING,
        /**
         * 终端正在优雅关闭中的状态。
         * <p>调用{@link #shutdown()}方法后，终端开始优雅关闭各个组件时的状态。</p>
         */
        SHUTTING_DOWN,
        /**
         * 终端已终止的状态。
         * <p>终端成功调用{@link #stop()}或{@link #shutdown()}方法后，所有组件都已关闭时的状态。</p>
         */
        TERMINATED,
        /**
         * 终端发生错误的状态。
         * <p>终端在启动、运行或关闭过程中发生异常时的状态。</p>
         */
        ERROR,
    }
    protected static final Logger logger = LoggerFactory.getLogger(LifecycleManager.class);
    protected final AtomicReference<State> state = new AtomicReference<>(LifecycleManager.State.NEW);
    protected final String name;

    protected LifecycleManager(String name) {
        this.name = name;
    }

    public final void start() {
        if (!state.compareAndSet(State.NEW, State.STARTING)) {
            throw new IllegalStateException("Cannot start " + name + " while in State " + state.get());
        }
        logger.info("{} starting", name);
        try {
            onStart();
            state.set(State.RUNNING);
            logger.info("{} start succeed", name);
        } catch (Exception e) {
            logger.error("{} start failed", name, e);
            state.set(State.ERROR);
            throw e;
        }
    }
    public final void stop() {
        if (!state.compareAndSet(State.RUNNING, State.STOPPING)) {
            throw new IllegalStateException("Cannot stop " + name + " while in State " + state.get());
        }
        logger.info("{} stopping", name);
        try {
            onStop();
            state.set(State.TERMINATED);
            logger.info("{} stop succeed", name);
        } catch (Exception e) {
            logger.error("{} stop failed", name, e);
            state.set(State.ERROR);
            throw e;
        }
    }
    public final void shutdown() {
        if (!state.compareAndSet(State.RUNNING, State.SHUTTING_DOWN)) {
            throw new IllegalStateException("Cannot shutdown " + name + " while in State " + state.get());
        }
        logger.info("{} shutting down", name);
        try {
            onShutdown();
            state.set(State.TERMINATED);
            logger.info("{} shutdown succeed", name);
        } catch (Exception e) {
            logger.error("{} shutdown failed", name, e);
            state.set(State.ERROR);
            throw e;
        }
    }

    public final State getState() {
        return state.get();
    }

    public abstract void onStart();
    public abstract void onStop();
    public abstract void onShutdown();
}
