package com.nebilux.nexalithic.core.model.packet.business;

import com.nebilux.nexalithic.core.messaging.task.visual.TransferSnapshot;
import com.nebilux.nexalithic.core.model.packet.AbstractPacket;
import com.nebilux.nexalithic.core.model.packet.business.payload.AbstractPayload;
import com.nebilux.nexalithic.core.model.packet.business.payload.TextPayload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务包
 *
 * @author Reonvia
 * @since 0.1.0
 */
public class BusinessPacket extends AbstractPacket {
    public static final int MAX_PATH_DEPTH = Byte.MAX_VALUE;
    public static final int BASE_HEADER_SIZE = Byte.BYTES * 2 + Short.BYTES + Long.BYTES * 2;
    /**
     * 消息处理方式。
     *
     * <p>用于标识请求类型以及响应结果类型。</p>
     *
     * <h2>Code 编码规则</h2>
     *
     * <pre>
     * 1xxxx - 请求（Request）
     *
     *   11000 - POST
     *   12000 - DELETE
     *   13000 - UPDATE
     *   14000 - GET
     *
     * 2xxxx - 响应（Response）
     *
     *   21xxx - 正常业务响应
     *       21000 - OK
     *       21100 - Success
     *       21200 - Failure
     *
     *   22xxx - 业务执行错误
     *       22000 - Error
     *
     *   23xxx - 请求错误
     *       23100 - Bad Request
     *       23200 - Unauthorized
     *       23300 - Forbidden
     *
     *   24xxx - 路由 / 资源错误
     *       24100 - Not Handler
     *       24200 - Not Resource
     *       24300 - Method Not Allowed
     *
     *   25xxx - 服务状态错误
     *       25100 - Busy
     * </pre>
     *
     * <h2>业务响应语义</h2>
     *
     * <pre>
     * Success
     *     业务流程正常结束，并且业务操作成功。
     *
     * Failure
     *     业务流程正常结束，但业务操作未成功。
     *     例如：密码错误、余额不足、用户名已存在。
     *
     * Error
     *     业务处理过程中发生异常，导致业务流程无法正常完成。
     *     例如：数据库异常、业务代码抛出未预期异常。
     * </pre>
     *
     * <p>
     * 因此 {@link #RESPONSE_Failure} 与 {@link #RESPONSE_Error}
     * 具有本质区别：
     * </p>
     *
     * <ul>
     *     <li>Failure：正常的业务结果</li>
     *     <li>Error：异常的业务执行过程</li>
     * </ul>
     */
    public enum Way {

        /*
         * ============================================================
         * Request
         * ============================================================
         */

        /**
         * POST 请求。
         *
         * <p>通常用于创建资源、提交数据或执行具有副作用的业务操作。</p>
         */
        REQUEST_Post(
                Type.REQUEST,
                (short) 11000
        ),

        /**
         * DELETE 请求。
         *
         * <p>通常用于删除指定资源。</p>
         */
        REQUEST_Delete(
                Type.REQUEST,
                (short) 12000
        ),

        /**
         * UPDATE 请求。
         *
         * <p>通常用于修改或更新已有资源。</p>
         */
        REQUEST_Update(
                Type.REQUEST,
                (short) 13000
        ),

        /**
         * GET 请求。
         *
         * <p>通常用于查询或获取资源。</p>
         */
        REQUEST_Get(
                Type.REQUEST,
                (short) 14000
        ),


        /*
         * ============================================================
         * Response - Normal
         * ============================================================
         */

        /**
         * 通用正常响应。
         *
         * <p>
         * 表示请求已经被正常接收和处理，业务处理流程没有发生异常。
         * </p>
         *
         * <p>
         * 此状态本身不强调具体业务操作是成功还是失败。
         * 如果需要明确业务结果，应优先使用
         * {@link #RESPONSE_Success} 或 {@link #RESPONSE_Failure}。
         * </p>
         */
        RESPONSE_Ok(
                Type.RESPONSE,
                (short) 21000
        ),

        /**
         * 业务成功。
         *
         * <p>
         * 表示业务处理流程正常结束，并且业务目标成功完成。
         * </p>
         *
         * <p>例如：</p>
         *
         * <ul>
         *     <li>用户登录成功</li>
         *     <li>资源创建成功</li>
         *     <li>文件上传成功</li>
         *     <li>数据更新成功</li>
         * </ul>
         */
        RESPONSE_Success(
                Type.RESPONSE,
                (short) 21100
        ),

        /**
         * 业务失败。
         *
         * <p>
         * 表示业务处理流程本身正常执行完成，但业务目标没有成功。
         * </p>
         *
         * <p>
         * Failure 属于一种正常且可预期的业务结果，
         * 并不意味着程序发生异常。
         * </p>
         *
         * <p>例如：</p>
         *
         * <ul>
         *     <li>用户名或密码错误</li>
         *     <li>用户名已经存在</li>
         *     <li>余额不足</li>
         *     <li>业务条件不满足</li>
         * </ul>
         */
        RESPONSE_Failure(
                Type.RESPONSE,
                (short) 21200
        ),


        /*
         * ============================================================
         * Response - Business Error
         * ============================================================
         */

        /**
         * 业务执行错误。
         *
         * <p>
         * 表示请求已经进入业务处理流程，但在执行过程中发生异常，
         * 导致业务流程无法正常完成。
         * </p>
         *
         * <p>例如：</p>
         *
         * <ul>
         *     <li>数据库访问异常</li>
         *     <li>业务 Service 抛出未预期异常</li>
         *     <li>外部服务调用异常</li>
         *     <li>业务执行过程中发生运行时错误</li>
         * </ul>
         *
         * <p>
         * 注意：业务条件不满足不应该返回 Error，
         * 而应该返回 {@link #RESPONSE_Failure}。
         * </p>
         */
        RESPONSE_Error(
                Type.RESPONSE,
                (short) 22000
        ),


        /*
         * ============================================================
         * Response - Request Error
         * ============================================================
         */

        /**
         * 请求错误。
         *
         * <p>
         * 表示客户端发送的请求本身不合法，
         * 导致服务器无法正常处理该请求。
         * </p>
         *
         * <p>例如：</p>
         *
         * <ul>
         *     <li>缺少必要参数</li>
         *     <li>参数格式错误</li>
         *     <li>Payload 无法解析</li>
         *     <li>请求数据结构不符合协议要求</li>
         * </ul>
         */
        RESPONSE_BadRequest(
                Type.RESPONSE,
                (short) 23100
        ),

        /**
         * 未认证。
         *
         * <p>
         * 表示当前请求需要身份认证，
         * 但客户端没有提供有效的身份认证信息。
         * </p>
         *
         * <p>例如：</p>
         *
         * <ul>
         *     <li>未登录</li>
         *     <li>Token 缺失</li>
         *     <li>Token 无效或已经过期</li>
         * </ul>
         */
        RESPONSE_Unauthorized(
                Type.RESPONSE,
                (short) 23200
        ),

        /**
         * 无访问权限。
         *
         * <p>
         * 表示客户端身份已经被确认，
         * 但当前身份没有执行该操作或访问该资源的权限。
         * </p>
         */
        RESPONSE_Forbidden(
                Type.RESPONSE,
                (short) 23300
        ),


        /*
         * ============================================================
         * Response - Routing / Resource Error
         * ============================================================
         */

        /**
         * 未找到处理器。
         *
         * <p>
         * 表示当前请求路径无法匹配到任何已注册的 Handler。
         * </p>
         */
        RESPONSE_NotHandler(
                Type.RESPONSE,
                (short) 24100
        ),

        /**
         * 未找到资源。
         *
         * <p>
         * 表示 Handler 或请求路径本身有效，
         * 但请求指定的目标资源不存在。
         * </p>
         */
        RESPONSE_NotResource(
                Type.RESPONSE,
                (short) 24200
        ),

        /**
         * 请求方式不允许。
         *
         * <p>
         * 表示目标处理器或资源存在，
         * 但不支持当前请求所使用的 {@link Way}。
         * </p>
         *
         * <p>例如：</p>
         *
         * <pre>
         * Handler 只允许 GET，
         * 客户端却使用 DELETE。
         * </pre>
         */
        RESPONSE_MethodNotAllowed(
                Type.RESPONSE,
                (short) 24300
        ),


        /*
         * ============================================================
         * Response - Server State
         * ============================================================
         */

        /**
         * 服务繁忙。
         *
         * <p>
         * 表示服务器当前暂时无法处理该请求，
         * 但并不意味着请求本身存在问题。
         * </p>
         *
         * <p>例如：</p>
         *
         * <ul>
         *     <li>服务器过载</li>
         *     <li>任务队列已满</li>
         *     <li>线程池无法继续接受任务</li>
         *     <li>服务器处于临时限流状态</li>
         * </ul>
         */
        RESPONSE_Busy(
                Type.RESPONSE,
                (short) 25100
        ),
        ;


        /**
         * 消息类型。
         *
         * <p>
         * 用于区分当前 {@link Way} 属于请求还是响应。
         * </p>
         */
        public enum Type {
            /**
             * 请求消息。
             */
            REQUEST,

            /**
             * 响应消息。
             */
            RESPONSE,
        }


        /**
         * 当前处理方式所属的消息类型。
         */
        public final Type type;
        /**
         * 当前处理方式对应的协议状态码。
         *
         * <p>
         * Code 不仅用于唯一标识 {@link Way}，
         * 同时按照数值区间携带一定的分类语义。
         * </p>
         */
        public final short code;

        Way(Type type, short code) {
            this.type = type;
            this.code = code;
        }

        /**
         * 判断当前消息是否为请求。
         *
         * @return 如果属于请求则返回 {@code true}
         */
        public boolean isRequest() {
            return type == Type.REQUEST;
        }

        /**
         * 判断当前消息是否为响应。
         *
         * @return 如果属于响应则返回 {@code true}
         */
        public boolean isResponse() {
            return type == Type.RESPONSE;
        }

        /**
         * 判断当前响应是否属于正常业务响应。
         *
         * <p>
         * 包括：
         * </p>
         *
         * <ul>
         *     <li>{@link #RESPONSE_Ok}</li>
         *     <li>{@link #RESPONSE_Success}</li>
         *     <li>{@link #RESPONSE_Failure}</li>
         * </ul>
         *
         * <p>
         * {@code Failure} 同样属于正常响应，
         * 因为它表示业务流程正常完成，只是业务结果失败。
         * </p>
         *
         * @return 是否属于正常业务响应
         */
        public boolean isOk() {
            return code >= 21000 && code < 22000;
        }

        /**
         * 判断当前响应是否表示业务执行过程中发生异常。
         *
         * @return 是否为业务执行错误
         */
        public boolean isError() {
            return code >= 22000 && code < 23000;
        }

        /**
         * 判断当前响应是否属于请求本身的错误。
         *
         * @return 是否属于请求错误
         */
        public boolean isRequestError() {
            return code >= 23000 && code < 24000;
        }

        /**
         * 判断当前响应是否属于路由或资源错误。
         *
         * @return 是否属于路由或资源错误
         */
        public boolean isRoutingError() {
            return code >= 24000 && code < 25000;
        }

        /**
         * 判断当前响应是否属于服务器状态错误。
         *
         * @return 是否属于服务器状态错误
         */
        public boolean isServerStateError() {
            return code >= 25000 && code < 26000;
        }

        /**
         * 判断业务结果是否明确表示成功。
         *
         * @return 当前状态是否为 {@link #RESPONSE_Success}
         */
        public boolean isSuccess() {
            return this == RESPONSE_Success;
        }

        /**
         * 判断业务结果是否明确表示失败。
         *
         * <p>
         * 这里的 Failure 是正常的业务失败结果，
         * 不代表执行过程中发生异常。
         * </p>
         *
         * @return 当前状态是否为 {@link #RESPONSE_Failure}
         */
        public boolean isFailure() {
            return this == RESPONSE_Failure;
        }
    }

    private static final Way[] WAYS = Way.values();
    private static final AtomicInteger counter = new AtomicInteger(0);
    private final int packetId = counter.getAndIncrement();
    private volatile boolean sealed = false;

    private long taskId = Long.MIN_VALUE;
    private long packetSize;
    private short way;
    private byte pathDepth;
    private short[] path;
    private byte payloadCount;
    private long[] payloadsMeta;
    private List<AbstractPayload<?>> payloads;

    private BusinessPacket(Way way, short... path) {
        this.way = (short) way.ordinal();
        if (path != null && path.length > 0) {
            if (path.length > MAX_PATH_DEPTH) {
                throw new IllegalArgumentException("path length exceeds maximum of " + MAX_PATH_DEPTH);
            }
            this.path = path;
        }
    }
    public static BusinessPacket create(Way way, short... path) {
        return new BusinessPacket(way, path);
    }

    public final BusinessPacket attach(AbstractPayload<?> payload) {
        if (sealed) {
            throw new IllegalStateException("Cannot attach payload to a sealed packet.");
        }
        if (payload != null) {
            if (this.payloads == null) {
                this.payloads = new ArrayList<>();
            }
            if (payloadCount + 1 > MAX_PAYLOAD_COUNT) {
                throw new PayloadOverflowException(payloadCount, 1, MAX_PAYLOAD_COUNT);
            }
            this.payloads.add(payload);
        }
        return this;
    }
    public final BusinessPacket attach(AbstractPayload<?>... payloads) {
        if (sealed) {
            throw new IllegalStateException("Cannot attach payload to a sealed packet.");
        }
        if (payloads != null && payloads.length > 0) {
            if (this.payloads == null) {
                this.payloads = new ArrayList<>();
            }
            int newCount = payloads.length;
            if (payloadCount + newCount > MAX_PAYLOAD_COUNT) {
                throw new PayloadOverflowException(payloadCount, newCount, MAX_PAYLOAD_COUNT);
            }
            for (AbstractPayload<?> payload : payloads) {
                if (payload != null) {
                    this.payloads.add(payload);
                }
            }
        }
        return this;
    }
    public final BusinessPacket attach(Collection<AbstractPayload<?>> payloads) {
        if (sealed) {
            throw new IllegalStateException("Cannot attach payload to a sealed packet.");
        }
        if (payloads != null && !payloads.isEmpty()) {
            if (this.payloads == null) {
                this.payloads = new ArrayList<>();
            }
            int newCount = payloads.size();
            if (payloadCount + newCount > MAX_PAYLOAD_COUNT) {
                throw new PayloadOverflowException(payloadCount, newCount, MAX_PAYLOAD_COUNT);
            }
            for (AbstractPayload<?> payload : payloads) {
                if (payload != null) {
                    this.payloads.add(payload);
                }
            }
        }
        return this;
    }

    /**
     * 封存报文：执行最后的一次性大小计算，并禁止后续修改。
     */
    public final BusinessPacket seal() {
        if (sealed) {
            return this;
        }
        long packetSize = BASE_HEADER_SIZE;
        if (path != null) {
            pathDepth = (byte) path.length;
            packetSize += (long) pathDepth * Short.BYTES;
        } else {
            pathDepth = 0;
        }
        if (payloads != null && !payloads.isEmpty()) {
            int count = payloads.size();
            payloadCount = (byte) count;
            payloadsMeta = new long[count * 2];
            for (int i = 0; i < count; i++) {
                AbstractPayload<?> payload = payloads.get(i);
                long payloadSize = payload.getTotalSize();
                payloadsMeta[i * 2] = payload.getPayloadUID();
                payloadsMeta[i * 2 + 1] = payloadSize;
                packetSize += payloadSize;
            }
            packetSize += (long) payloadsMeta.length * Long.BYTES;
        } else {
            payloadCount = 0;
        }
        this.packetSize = packetSize;
        this.sealed = true;
        return this;
    }

    /**
     * 深度克隆报文结构，但保持 Payload 数据的引用。
     * 专门用于 pushToAll 场景。
     */
    public BusinessPacket duplicate() {
        if (!this.sealed) {
            throw new IllegalStateException("Only sealed packets can be duplicated for broadcast.");
        }
        BusinessPacket clone = new BusinessPacket(this.getWay(), this.path);
        clone.taskId = this.taskId;
        clone.packetSize = this.packetSize;
        clone.pathDepth = this.pathDepth;
        clone.payloadCount = this.payloadCount;
        clone.payloadsMeta = this.payloadsMeta;
        if (this.payloads != null) {
            clone.payloads = new ArrayList<>(this.payloadCount);
            for (AbstractPayload<?> p : this.payloads) {
                clone.payloads.add(p.duplicate());
            }
        }
        clone.sealed = true;
        return clone;
    }

    public final List<AbstractPayload<?>> payloads() {
        return payloads;
    }
    @SuppressWarnings("unchecked")
    public final <P extends AbstractPayload<?>> P payload(int index) {
        if (payloads == null || payloads.isEmpty() || index >= payloadCount) {
            return null;
        }
        return (P) payloads.get(index);
    }
    public final AbstractPayload<?> firstPayload() {
        if (payloads == null || payloads.isEmpty()) {
            return null;
        }
        return payloads.getFirst();
    }
    public final AbstractPayload<?> lastPayload() {
        if (payloads == null || payloads.isEmpty()) {
            return null;
        }
        return payloads.getLast();
    }
    public final Way getWay() {
        return WAYS[way];
    }

    public final int getHeaderSize() {
        return BASE_HEADER_SIZE + pathDepth * Short.BYTES + payloadCount * Long.BYTES * 2;
    }
    public final int getPacketId() {
        return packetId;
    }
    public final long getTaskId() {
        return taskId;
    }
    public final long getPacketSize() {
        return packetSize;
    }
    public final short getWayCode() {
        return way;
    }
    public final short[] getPath() {
        return path;
    }
    public final byte getPathDepth() {
        return pathDepth;
    }
    public final byte getPayloadCount() {
        return payloadCount;
    }
    public final long[] getPayloadsMeta() {
        return payloadsMeta;
    }
    public final long getTotalPayloadSize() {
        return getPacketSize() - getHeaderSize();
    }

    public final BusinessPacket setTaskId(long taskId) {
        this.taskId = taskId;
        return this;
    }

    @Override
    public final PacketType packetType() {
        return PacketType.Business;
    }

    public String getDisplayMessage() {
        return switch (firstPayload()) {
            case null -> "";
            case TextPayload tp -> tp.value();
            case Object other -> other.toString();
        };
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Way: ").append(WAYS[way]).append(", Path: ").append(Arrays.toString(path)).append(", TaskId: ").append(taskId)
                .append(", PacketSize: ").append(TransferSnapshot.formatSize(packetSize)).append(", PayloadCount: ").append(payloadCount);
        if (payloads != null) {
            sb.append(", Payloads: { ");
            for (AbstractPayload<?> payload : payloads) {
                sb.append(payload).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append(" }");
        }
        return sb.toString();
    }
}
