package com.thezeroer.nexalithic.server.io.accept;

import com.thezeroer.nexalithic.core.io.channel.NexalithicChannel;

import java.nio.channels.SocketChannel;

/**
 * 准入上下文分配器。
 *
 * <p>为每次连接准入过程提供一个用户定义的上下文。同一次准入过程中的所有
 * {@link AdmissionFilter} 共享同一个上下文，可通过它传递过滤器之间的中间状态。</p>
 *
 * <p>实现既可以创建普通对象，也可以从对象池中获取对象。池化实现应保证
 * {@link #acquire(NexalithicChannel.Kind, SocketChannel)} 与 {@link #release(Object)}
 * 成对工作，并在释放时清除上一次使用遗留的状态。</p>
 *
 * <p>过滤链不需要上下文时可以返回 {@code null}；此时所有过滤器都必须允许
 * {@code context} 参数为 {@code null}。</p>
 *
 * @param <C> 准入上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/09/26
 */
@FunctionalInterface
public interface AdmissionContextAllocator<C> {

    /**
     * 为一次连接准入过程获取上下文。
     *
     * <p>该方法由实际执行准入任务的线程调用。如果准入策略采用异步并发调度，
     * 实现需要满足相应的线程安全要求。</p>
     *
     * @param kind    待准入连接的通道种类
     * @param channel 已由监听器接受、尚未移交给握手阶段的通道
     * @return 本次准入过程使用的上下文；不需要上下文时可以返回 {@code null}
     */
    C acquire(NexalithicChannel.Kind kind, SocketChannel channel);

    /**
     * 释放上下文。
     *
     * <p>池化实现应在此处重置并归还对象；由垃圾回收器管理的普通对象可以使用默认空实现。</p>
     *
     * <p>该方法只负责上下文资源，不应关闭 {@link SocketChannel}。
     * 连接所有权由 {@link AdmissionStrategy} 管理。</p>
     *
     * @param context 需要释放的上下文，可能为 {@code null}
     */
    default void release(C context) {}
}