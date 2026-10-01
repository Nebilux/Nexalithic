package com.nebilux.nexalithic.core.model.packet.business.payload;

import com.nebilux.nexalithic.core.infra.buffer.LoopBuffer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.zip.CRC32;

/**
 * 业务有效载荷基类。
 * <p>本类定义了数据在业务实体（T）与二进制流（LoopBufferView）之间转换的标准流程。
 * 支持大对象的分段编解码。</p>
 *
 * @param <T> 业务数据类型（如 Map, String 或自定义 POJO）
 * @author Reonvia
 * @since 0.1.0
 */
public abstract class AbstractPayload<T> {
    /** 具体的业务数据对象 */
    protected volatile T value;
    /** 数据的总字节大小（用于进度控制和长度校验） */
    protected volatile long totalSize;
    /** 当前已编解码处理的字节大小 */
    protected volatile long processedSize;

    public AbstractPayload() {}

    /**
     * 设置业务数据对象。
     * @param value 数据实例
     */
    public void value(T value) {
        this.value = value;
    }

    /**
     * 获取业务数据对象。
     * @return T
     */
    public final T value() {
        return value;
    }

    /**
     * 获取 Payload 的实际总字节大小。
     * <p>在编码时，该值决定了需要写出多少数据；在解码时，该值通常由报文头通知。</p>
     * @return 字节总数
     */
    public final long getTotalSize() {
        return totalSize;
    }

    /**
     * 获取当前已完成读/写的字节数。
     * <p>在流式传输大对象（如文件）时，可用于计算进度或驱动分段写入逻辑。</p>
     * @return 已处理字节数
     */
    public final long getProcessedSize() {
        return processedSize;
    }

    /**
     * <b>编码动作：</b>将业务对象（data）转换为二进制流并写入缓冲区。
     * <p>框架可能会多次调用此方法，直到 {@code processedSize == totalSize}。
     *
     * @param output 目标输出视图
     * @return 本次实际写入的字节数
     * @throws IOException 如果业务逻辑转换出错
     */
    public abstract int encode(LoopBuffer.LimitedWritableView output) throws IOException;

    /**
     * <b>解码动作：</b>从缓冲区读取二进制数据并填充/解析为业务对象（data）。
     * <p>框架可能会多次调用此方法，直到 {@code processedSize == totalSize}。
     *
     * @param input 源输入视图
     * @return 本次实际读取并解析的字节数
     * @throws IOException 如果格式校验或解析出错
     */
    public abstract int decode(LoopBuffer.LimitedReadableView input) throws IOException;

    /**
     * 创建一个指向相同数据源，但进度重置为 0 的新实例。
     * 用于多线程并发发送同一报文。
     */
    public abstract AbstractPayload<T> duplicate();

    /**
     * 获取有效载荷的唯一标识（UID）。
     * <p>默认实现基于类名及字段结构生成 CRC32 校验码，用于协议版本识别和类型安全校验。
     * <b>建议：</b>在生产环境中重写此方法，返回一个固定的常量 {@code long} 值以提升性能。</p>
     *
     * @return 64 位唯一标识符
     */
    public long getPayloadUID() {
        Class<?> clazz = getClass();
        StringBuilder sb = new StringBuilder();
        sb.append(clazz.getName());
        Field[] fields = clazz.getDeclaredFields();
        Arrays.sort(fields, Comparator.comparing(Field::getName));
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic()) {
                sb.append("|").append(field.getName()).append(":").append(field.getType().getSimpleName());
            }
        }
        CRC32 crc = new CRC32();
        crc.update(sb.toString().getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }

    /**
     * 编码准备钩子。
     * <p>在第一次调用 {@link #encode(LoopBuffer.LimitedWritableView)} 之前触发。用于打开资源或初始化编码状态机。</p>
     */
    public void prepareEncode() throws IOException {}

    /**
     * 编码完成钩子。
     * <p>在所有数据编码完毕（processedSize == totalSize）后触发。</p>
     */
    public void finishEncode() throws IOException {}

    /**
     * 解码准备钩子。
     * <p>在收到报文长度并准备开始解码前触发。常用于初始化业务对象容器（如 {@code new T()}）。</p>
     *
     * @param totalSize 报文头部声明的总数据大小
     */
    public void prepareDecode(long totalSize) throws IOException {
        this.totalSize = totalSize;
    }

    /**
     * 解码完成钩子。
     * <p>在成功解析完整数据后触发。常用于执行业务逻辑前的最终校验或数据合并。</p>
     */
    public void finishDecode() throws IOException {}

    /**
     * 资源清理钩子。
     * <p>无论操作成功还是异常，在 Payload 生命周期结束时触发。
     * 务必在此释放引用的池化对象或其他外部资源（如临时文件句柄）。</p>
     */
    public void release() {}
}