package com.thezeroer.nexalithic.server.io.accept;

import com.thezeroer.nexalithic.core.io.channel.NexalithicChannel;

import java.nio.channels.SocketChannel;

/**
 * 连接准入过滤器。
 *
 * <p>过滤器按照其在 {@link AdmissionStrategy} 中的注册顺序依次执行。
 * 每个过滤器可以允许连接继续、拒绝连接，或者通过抛出异常进入异常传播路径。</p>
 *
 * <p>过滤器只负责判断，不应主动关闭 SocketChannel，也不应将连接提交给握手阶段。
 * 连接的关闭和所有权转移统一由 AdmissionStrategy 处理。</p>
 *
 * <p>当某个过滤器抛出异常后，策略会从下一个过滤器开始调用
 * {@link #onFailure(NexalithicChannel.Kind, SocketChannel, Object, Exception)}。
 * 后续过滤器可以恢复正常流程、拒绝连接或者继续传播异常。</p>
 *
 * <p>同一个过滤器实例可能同时服务于多个连接。如果策略采用异步并发调度，
 * 过滤器应保持无状态，或者自行保证内部状态的线程安全。</p>
 *
 * @param <C> 准入上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/26
 */
public interface AdmissionFilter<C> {
    /**
     * 判断连接是否允许继续执行后续过滤器。
     *
     * @param kind    待准入连接的通道种类
     * @param channel 待准入的通道
     * @param context 本次准入过程共享的上下文，可能为 {@code null}
     * @return {@code true} 表示继续执行过滤链；
     *         {@code false} 表示拒绝连接
     * @throws Exception 过滤过程发生异常；异常将从下一个过滤器开始传播
     */
    boolean allow(NexalithicChannel.Kind kind, SocketChannel channel, C context) throws Exception;

    /**
     * 处理前一个过滤器产生并向后传播的异常。
     *
     * <p>该方法只在过滤链进入异常传播路径后调用。</p>
     *
     * <p>返回 {@link FailureAction#RECOVER} 后，当前异常被视为已经处理，
     * 策略会从下一个过滤器恢复正常的 {@link #allow} 调用。当前过滤器不会再为同一个连接
     * 执行自己的 {@code allow} 方法。</p>
     *
     * @param kind    待准入连接的通道种类
     * @param channel 待准入的通道
     * @param context 本次准入过程共享的上下文，可能为 {@code null}
     * @param cause   当前正在传播的异常
     * @return 异常处理动作，不能为 {@code null}
     * @throws Exception 异常处理过程产生的新异常；
     *                   新异常将替换当前异常继续向后传播
     */
    default FailureAction onFailure(NexalithicChannel.Kind kind, SocketChannel channel, C context, Exception cause) throws Exception {
        return FailureAction.PROPAGATE;
    }

    /**
     * 过滤异常的处理动作。
     */
    enum FailureAction {

        /**
         * 当前异常已经处理完成。
         *
         * <p>策略清除当前异常，并从下一个过滤器恢复正常过滤流程。</p>
         */
        RECOVER,

        /**
         * 拒绝当前连接。
         *
         * <p>策略终止过滤链并关闭 SocketChannel。</p>
         */
        REJECT,

        /**
         * 当前过滤器不处理该异常。
         *
         * <p>策略保留当前异常，并将其传递给下一个过滤器。</p>
         */
        PROPAGATE
    }
}