package com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation;

import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 拦截器绑定元注解。
 *
 * <p>用于将一个自定义业务注解与指定的
 * {@link HandlerInterceptor} 实现类型建立静态绑定关系。</p>
 *
 * <p>该注解只能标注在其他注解类型上，不能直接应用于
 * Controller 类或 Handler 方法。Controller 装配器扫描类和方法上的
 * 自定义注解时，会读取其注解类型上的 {@code InterceptorBinding}，
 * 从而确定需要创建并应用的拦截器类型。</p>
 *
 * <p><strong>注意：</strong>被该元注解标记的自定义注解必须使用
 * {@link RetentionPolicy#RUNTIME} 保留策略，否则运行时装配器无法通过
 * 反射发现该自定义注解。</p>
 *
 * <p>本注解负责声明“自定义注解对应哪一种拦截器”以及同一层级内的相对执行顺序，
 * 但不负责：</p>
 *
 * <ul>
 *     <li>解析自定义注解中的配置参数；</li>
 *     <li>创建拦截器实例；</li>
 *     <li>管理拦截器实例的共享范围或生命周期。</li>
 * </ul>
 *
 * <p>典型定义：</p>
 *
 * <pre>{@code
 * @Documented
 * @Retention(RetentionPolicy.RUNTIME)
 * @Target({ElementType.TYPE, ElementType.METHOD})
 * @InterceptorBinding(AuthenticationInterceptor.class)
 * public @interface Authenticated {
 *     String[] roles() default {};
 * }
 * }</pre>
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/28
 *
 * @see HandlerInterceptor
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface InterceptorBinding {

    /**
     * 指定当前自定义注解所绑定的拦截器实现类型。
     *
     * <p>Controller 装配器通常以该类型作为 Key，查找已经注册的
     * 拦截器创建器，并由创建器生成与当前 Handler 上下文类型兼容的
     * {@link HandlerInterceptor} 实例。</p>
     *
     * <p>该属性保存的是拦截器的运行时类型标识。由于 Java 注解不能声明
     * 与 Controller 装配器相同的泛型上下文参数，因此这里使用
     * {@code HandlerInterceptor<?>} 表示任意上下文类型的拦截器。
     * 实际上下文类型兼容性应由拦截器创建器和装配器负责保证。</p>
     *
     * @return 与当前自定义注解绑定的拦截器实现类型
     */
    Class<? extends HandlerInterceptor<?>> value();

    /**
     * 当前拦截器在同一作用域内的执行顺序。
     *
     * <p>数值越小，越早执行{@code onBefore}；
     * {@code onAfter}和{@code onCompletion}通常按照相反顺序执行。</p>
     *
     * @return 执行顺序
     */
    int order() default 0;
}
