package com.nebilux.nexalithic.server.io.accept;

import com.nebilux.nexalithic.core.io.channel.NexalithicChannel;

import java.nio.channels.SocketChannel;

/**
 * 准入任务调度器。
 *
 * <p>用于决定准入任务的执行位置和执行时机。实现可以在调用线程中直接执行任务，
 * 也可以将任务提交给线程池、专用线程或自定义队列。</p>
 *
 * <p>Dispatcher 只负责调度，不负责执行具体过滤规则，也不负责将连接提交给握手阶段。</p>
 *
 * <h2>返回值与所有权约定</h2>
 * <ul>
 *     <li>
 *         返回 {@code true}：Dispatcher 已经接收任务，并保证
 *         {@link AdmissionExecutor#execute(NexalithicChannel.Kind, SocketChannel)}
 *         最多执行一次。
 *     </li>
 *     <li>
 *         返回 {@code false}：任务没有执行，也没有被 Dispatcher 保留，
 *         连接所有权仍属于 {@link AdmissionStrategy}。
 *     </li>
 * </ul>
 *
 * <p>实现不得在已经执行或保留任务后返回 {@code false}，否则 AdmissionStrategy
 * 可能关闭已经进入过滤流程或已经移交给握手阶段的连接。</p>
 *
 * <p>异步实现还应保证：成功接收的任务最终必须被执行或在关闭阶段显式拒绝，
 * 不能静默丢弃任务及其持有的 SocketChannel。</p>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/26
 */
@FunctionalInterface
public interface AdmissionDispatcher {

    /**
     * 调度一次准入任务。
     *
     * @param executor 准入任务执行入口
     * @param kind     待准入连接的通道种类
     * @param channel  待准入的通道
     * @return {@code true} 表示任务已被接收；
     *         {@code false} 表示任务未执行且未被保留
     */
    boolean dispatch(AdmissionExecutor executor, NexalithicChannel.Kind kind, SocketChannel channel);
}