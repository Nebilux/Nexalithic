package com.thezeroer.nexalithic.core.messaging.handler.assembly;

import com.thezeroer.nexalithic.core.exception.NexalithicDuplicateKeyException;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerContext;
import com.thezeroer.nexalithic.core.messaging.handler.HandlerInvoker;
import com.thezeroer.nexalithic.core.messaging.handler.NexalithicHandler;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation.InterceptorBinding;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation.NexalithicHandlerController;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.annotation.NexalithicHandlerMethod;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorAnnotationResolver;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorConfigurationBinding;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.interceptor.InterceptorFactory;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverter;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.argument.HandlerMethodArgumentConverterSelector;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverter;
import com.thezeroer.nexalithic.core.messaging.handler.assembly.method.result.HandlerMethodResultConverterSelector;
import com.thezeroer.nexalithic.core.messaging.handler.interceptor.HandlerInterceptor;
import com.thezeroer.nexalithic.core.messaging.handler.mapping.HandlerPathMatcher;
import com.thezeroer.nexalithic.core.messaging.handler.mapping.HandlerRegistry;
import com.thezeroer.nexalithic.core.util.BeanFactory;
import com.thezeroer.nexalithic.core.util.ClassScanner;

import java.lang.annotation.Annotation;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.*;

/**
 * 基于注解的 Controller Handler 装配器。
 *
 * <p>装配器负责扫描已经注册的 Controller 实例，把标记了 {@link NexalithicHandlerMethod}
 * 的实例方法转换为 {@link NexalithicHandler}，并注册到 {@link HandlerRegistry.Builder} 中。</p>
 *
 * <p>面向使用者的主要入口不是直接持有该类型，而是通过 {@code NexalithicServer.Builder}
 * 或 {@code NexalithicClient.Builder} 的 {@code controllerHandlerAssemblerConfigurer(...)}
 * 方法取得 {@link Builder} 和 {@link ControllerHandlerAssemblerHelper}：</p>
 *
 * <pre>{@code
 * builder.controllerHandlerAssemblerConfigurer((assembler, helper) -> {
 *     helper.defaultHandlerMethodConverterSelector(assembler)
 *             .controller(new UserController());
 * });
 * }</pre>
 *
 * <p>上层 Builder 会在 {@code build()} 阶段调用本装配器的 {@link #assembleInto(HandlerRegistry.Builder)}，
 * 因此用户通常只需要在回调中注册 Controller 和配置转换器、拦截器，不需要手动调用装配方法。</p>
 *
 * <p>Controller 方法必须声明在 {@code public} 类中，并且自身必须是 {@code public} 实例方法。
 * 当方法形态正好是 {@code void handle(HC context)} 时，装配器会直接通过 {@link LambdaMetafactory}
 * 生成 {@link HandlerInvoker}，不经过参数转换器或结果转换器。其他方法形态会先在装配阶段选择参数转换器和
 * 结果转换器，再通过一个由 {@link LambdaMetafactory} 生成的桥接 {@link HandlerInvoker} 在运行时完成调用。</p>
 *
 * <p>如果所有 Handler 方法都采用 {@code public void method(HC context)} 形态，可以只注册 Controller。
 * 如果 Handler 方法声明了业务参数或返回值，则需要配置参数转换器和结果转换器。最常用的做法是调用
 * {@link ControllerHandlerAssemblerHelper#defaultHandlerMethodConverterSelector(Builder)}
 * 启用默认方法转换器。</p>
 *
 * <p>当 Controller 或方法上存在被 {@link InterceptorBinding} 标记的业务注解时，装配器会先通过
 * {@link InterceptorAnnotationResolver} 解析配置，再通过 {@link InterceptorFactory} 取得实际拦截器实例。</p>
 *
 * @param <HC> 当前装配器支持的 Handler 上下文类型
 *
 * @author tbrtz647@outlook.com
 * @version 1.0.0
 * @since 2026/07/11
 */
public class ControllerHandlerAssembler<HC extends HandlerContext<?>> {
    public static class Builder<HC extends HandlerContext<?>> {
        private final Class<HC> handlerContextType;
        private final Set<Object> controllers;
        private InterceptorAnnotationResolver annotationResolver;
        private InterceptorFactory<HC> interceptorFactory;
        private HandlerMethodArgumentConverterSelector<HC> argumentConverterSelector;
        private HandlerMethodResultConverterSelector<HC> resultConverterSelector;

        /**
         * 创建指定 Handler 上下文类型的装配器构建器。
         *
         * <p>用户代码通常通过 {@code controllerHandlerAssemblerConfigurer(...)} 回调获得该构建器，
         * 而不是直接调用本构造方法。</p>
         *
         * @param handlerContextType 当前装配器支持的 Handler 上下文类型
         */
        private Builder(Class<HC> handlerContextType) {
            this.handlerContextType = handlerContextType;
            this.controllers = new LinkedHashSet<>();
        }

        /**
         * 注册一个 Controller 实例。
         *
         * <p>Controller 类必须标记 {@link NexalithicHandlerController}。装配器会扫描该实例类中
         * 标记了 {@link NexalithicHandlerMethod} 的方法，并按 Controller 路径加方法路径注册。</p>
         *
         * @param controller 标记了 {@link NexalithicHandlerController} 的 Controller 实例
         * @return 当前构建器
         * @throws IllegalArgumentException 目标实例不是 Handler Controller 时抛出
         * @throws NexalithicDuplicateKeyException 重复注册同一实例时抛出
         */
        public Builder<HC> controller(Object controller) {
            if (!controller.getClass().isAnnotationPresent(NexalithicHandlerController.class)) {
                throw new IllegalArgumentException(controller.getClass().getName() + " is not annotated with @NexalithicHandlerController");
            }
            if (!controllers.add(controller)) {
                throw new NexalithicDuplicateKeyException("controllerSet", controller);
            }
            return this;
        }

        /**
         * 批量注册 Controller 实例。
         *
         * <p>集合中的每个实例都会按 {@link #controller(Object)} 的规则逐一注册。</p>
         *
         * @param controllers Controller 实例集合
         * @return 当前构建器
         */
        public Builder<HC> controllers(Collection<Object> controllers) {
            for (Object controller : controllers) {
                controller(controller);
            }
            return this;
        }

        /**
         * 扫描指定包下的 Controller 类型并从 Bean 工厂取得实例。
         *
         * <p>该方法只扫描被 {@link NexalithicHandlerController} 标记的类型。
         * 实例由传入的 {@link BeanFactory} 提供，便于接入调用方自己的对象创建或依赖注入策略。</p>
         *
         * @param packageName 待扫描的 Java 包名
         * @param factory 用于取得 Controller 实例的 Bean 工厂
         * @return 当前构建器
         */
        public Builder<HC> scanControllers(String packageName, BeanFactory factory) {
            List<Class<?>> classes = ClassScanner.scan(packageName);
            for (Class<?> clazz : classes) {
                NexalithicHandlerController handlerController = clazz.getAnnotation(NexalithicHandlerController.class);
                if (handlerController == null) {
                    continue;
                }
                controller(factory.getBean(clazz));
            }
            return this;
        }

        /**
         * 设置拦截器工厂。
         *
         * <p>只有当 Controller 类或 Handler 方法上使用了被 {@link InterceptorBinding}
         * 标记的业务注解时才需要配置。装配器会把注解解析结果交给该工厂，取得实际挂载到 Handler 上的
         * {@link HandlerInterceptor} 实例。</p>
         *
         * @param factory 拦截器工厂
         * @return 当前构建器
         */
        public Builder<HC> interceptorFactory(InterceptorFactory<HC> factory) {
            this.interceptorFactory = factory;
            return this;
        }

        /**
         * 设置拦截器注解解析器。
         *
         * <p>只有当 Controller 类或 Handler 方法上使用了被 {@link InterceptorBinding}
         * 标记的业务注解时才需要配置。解析器负责把用户自定义注解转换为拦截器工厂可识别的配置绑定。</p>
         *
         * @param resolver 拦截器注解解析器
         * @return 当前构建器
         */
        public Builder<HC> annotationResolver(InterceptorAnnotationResolver resolver) {
            this.annotationResolver = resolver;
            return this;
        }

        /**
         * 设置 Handler 方法参数转换器选择器。
         *
         * <p>当 Handler 方法不是 {@code public void method(HC context)} 的直接调用形态时，
         * 装配器会在装配阶段为每个形参选择一个 {@link HandlerMethodArgumentConverter}。</p>
         *
         * @param selector 参数转换器选择器
         * @return 当前构建器
         */
        public Builder<HC> argumentConverterSelector(HandlerMethodArgumentConverterSelector<HC> selector) {
            this.argumentConverterSelector = selector;
            return this;
        }

        /**
         * 设置 Handler 方法返回值转换器选择器。
         *
         * <p>当 Handler 方法存在返回值时，装配器会在装配阶段选择一个
         * {@link HandlerMethodResultConverter}。运行时该转换器负责处理 Controller 方法返回值，
         * 通常会创建响应包并通过当前 Handler 上下文发送。</p>
         *
         * @param selector 返回值转换器选择器
         * @return 当前构建器
         */
        public Builder<HC> resultConverterSelector(HandlerMethodResultConverterSelector<HC> selector) {
            this.resultConverterSelector = selector;
            return this;
        }

        /**
         * 构建 Controller Handler 装配器。
         *
         * <p>构建后 Controller 集合会被冻结。缺少转换器或拦截器组件不会在此处立即失败；
         * 当实际装配的 Handler 方法或拦截器绑定需要这些组件时，{@link #assembleInto(HandlerRegistry.Builder)}
         * 会抛出明确异常。</p>
         *
         * @return Controller Handler 装配器
         */
        public ControllerHandlerAssembler<HC> build() {
            return new ControllerHandlerAssembler<>(handlerContextType, Collections.unmodifiableSet(controllers), annotationResolver, interceptorFactory, argumentConverterSelector, resultConverterSelector);
        }
    }

    private final Class<HC> handlerContextType;
    private final Set<Object> controllers;
    private final InterceptorAnnotationResolver annotationResolver;
    private final InterceptorFactory<HC> interceptorFactory;
    private final HandlerMethodArgumentConverterSelector<HC> argumentConverterSelector;
    private final HandlerMethodResultConverterSelector<HC> resultConverterSelector;

    private ControllerHandlerAssembler(Class<HC> handlerContextType, Set<Object> controllers,
                                       InterceptorAnnotationResolver annotationResolver, InterceptorFactory<HC> interceptorFactory,
                                       HandlerMethodArgumentConverterSelector<HC> argumentConverterSelector, HandlerMethodResultConverterSelector<HC> resultConverterSelector) {
        this.handlerContextType = handlerContextType;
        this.controllers = controllers;
        this.annotationResolver = annotationResolver;
        this.interceptorFactory = interceptorFactory;
        this.argumentConverterSelector = argumentConverterSelector;
        this.resultConverterSelector = resultConverterSelector;
    }

    /**
     * 创建指定 Handler 上下文类型的装配器构建器。
     *
     * @param handlerContextType Handler 上下文类型
     * @param <HC> Handler 上下文类型
     * @return Controller Handler 装配器构建器
     */
    public static <HC extends HandlerContext<?>> Builder<HC> builder(Class<HC> handlerContextType) {
        return new Builder<>(handlerContextType);
    }

    /**
     * 将已注册 Controller 中的 Handler 方法装配到注册表构建器。
     *
     * <p>该方法由 {@code NexalithicServer.Builder} 和 {@code NexalithicClient.Builder}
     * 在构建阶段调用。它会解析 Controller 路径、创建方法调用器、创建拦截器列表，
     * 并把最终的 {@link NexalithicHandler} 注册到传入的注册表构建器。</p>
     *
     * @param registryBuilder Handler 注册表构建器
     * @throws Throwable Handler 方法适配或注册失败时抛出
     */
    public void assembleInto(HandlerRegistry.Builder<HC> registryBuilder) throws Throwable {
        for (Object controller : controllers) {
            Class<?> clazz = controller.getClass();
            NexalithicHandlerController handlerController = clazz.getAnnotation(NexalithicHandlerController.class);
            List<InterceptorAnnotationBinding> controllerInterceptorBinding = findInterceptorBindings(clazz);
            for (Method method : clazz.getDeclaredMethods()) {
                NexalithicHandlerMethod handlerMethod = method.getAnnotation(NexalithicHandlerMethod.class);
                if (handlerMethod == null) {
                    continue;
                }
                HandlerPathMatcher pathMatcher = HandlerPathMatcherParser.parse(handlerController, handlerMethod);
                List<InterceptorAnnotationBinding> handlerInterceptorBinding = findInterceptorBindings(method);
                registryBuilder.handler(NexalithicHandler.<HC>builder()
                        .pathMatcher(pathMatcher)
                        .invoker(createInvoker(controller, method))
                        .interceptors(createInterceptors(controllerInterceptorBinding, handlerInterceptorBinding))
                        .name(clazz.getSimpleName() + "#" + method.getName())
                        .build());
            }
        }
    }

    private static List<InterceptorAnnotationBinding> findInterceptorBindings(AnnotatedElement element) {
        Annotation[] annotations = element.getDeclaredAnnotations();
        if (annotations.length == 0) {
            return List.of();
        }
        List<InterceptorAnnotationBinding> bindings = new ArrayList<>(annotations.length);
        for (Annotation annotation : annotations) {
            for (InterceptorBinding interceptorBinding : annotation.annotationType().getDeclaredAnnotationsByType(InterceptorBinding.class)) {
                bindings.add(new InterceptorAnnotationBinding(interceptorBinding.value(), annotation, interceptorBinding.order()));
            }
        }
        bindings.sort(Comparator.comparingInt(InterceptorAnnotationBinding::order));
        return bindings;
    }

    private HandlerInvoker<HC> createInvoker(Object bean, Method method) throws Throwable {
        validateBasicHandlerMethod(bean, method);
        if (isDirectHandlerMethod(method)) {
            return createDirectInvoker(bean, method);
        }
        return createConvertedInvoker(bean, method);
    }

    private void validateBasicHandlerMethod(Object bean, Method method) {
        Class<?> declaringType = method.getDeclaringClass();

        if (!declaringType.isInstance(bean)) {
            throw new IllegalArgumentException(
                    "Bean type '%s' is not compatible with handler method declaring type '%s': %s"
                            .formatted(bean.getClass().getName(), declaringType.getName(), method)
            );
        }

        if (!Modifier.isPublic(declaringType.getModifiers())) {
            throw new IllegalArgumentException("Handler controller class must be public: " + declaringType.getName());
        }

        int methodModifiers = method.getModifiers();
        if (Modifier.isStatic(methodModifiers)) {
            throw new IllegalArgumentException("Handler method must be an instance method: " + method);
        }
        if (Modifier.isAbstract(methodModifiers)) {
            throw new IllegalArgumentException("Handler method must not be abstract: " + method);
        }
        if (Modifier.isNative(methodModifiers)) {
            throw new IllegalArgumentException("Handler method must not be native: " + method);
        }
        if (!Modifier.isPublic(methodModifiers)) {
            throw new IllegalArgumentException("Handler method must be public: " + method);
        }
        if (method.isBridge()) {
            throw new IllegalArgumentException("Handler method must not be a bridge method: " + method);
        }
        if (method.isSynthetic()) {
            throw new IllegalArgumentException("Handler method must not be synthetic: " + method);
        }
    }

    private boolean isDirectHandlerMethod(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        return method.getReturnType() == void.class
                && parameterTypes.length == 1
                && HandlerContext.class.isAssignableFrom(parameterTypes[0])
                && parameterTypes[0].isAssignableFrom(handlerContextType);
    }

    private HandlerInvoker<HC> createDirectInvoker(Object bean, Method method) throws Throwable {
        Class<?> declaringType = method.getDeclaringClass();
        Class<?> methodContextType = method.getParameterTypes()[0];
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodHandle implementationMethod = lookup.unreflect(method);

        CallSite callSite = LambdaMetafactory.metafactory(
                lookup,
                "invoke",
                MethodType.methodType(HandlerInvoker.class, declaringType),
                MethodType.methodType(void.class, HandlerContext.class),
                implementationMethod,
                MethodType.methodType(void.class, methodContextType)
        );

        Object result = callSite.getTarget().invoke(bean);

        @SuppressWarnings("unchecked")
        HandlerInvoker<HC> invoker = (HandlerInvoker<HC>) result;
        return invoker;
    }

    private HandlerInvoker<HC> createConvertedInvoker(Object bean, Method method) throws Throwable {
        HandlerMethodInvocationPlan<HC> plan = createInvocationPlan(bean, method);
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodHandle implementationMethod = lookup.findStatic(
                ControllerHandlerAssembler.class,
                "invokeWithConverters",
                MethodType.methodType(void.class, HandlerMethodInvocationPlan.class, HandlerContext.class)
        );

        CallSite callSite = LambdaMetafactory.metafactory(
                lookup,
                "invoke",
                MethodType.methodType(HandlerInvoker.class, HandlerMethodInvocationPlan.class),
                MethodType.methodType(void.class, HandlerContext.class),
                implementationMethod,
                MethodType.methodType(void.class, HandlerContext.class)
        );

        Object result = callSite.getTarget().invoke(plan);

        @SuppressWarnings("unchecked")
        HandlerInvoker<HC> invoker = (HandlerInvoker<HC>) result;
        return invoker;
    }

    private HandlerMethodInvocationPlan<HC> createInvocationPlan(Object bean, Method method) throws IllegalAccessException {
        Parameter[] parameters = method.getParameters();
        HandlerMethodArgumentConverter<HC>[] argumentConverters = selectArgumentConverters(method, parameters);
        HandlerMethodResultConverter<HC> resultConverter = selectResultConverter(method);
        MethodHandle invoker = MethodHandles.lookup()
                .unreflect(method)
                .bindTo(bean)
                .asSpreader(Object[].class, parameters.length);
        return new HandlerMethodInvocationPlan<>(
                method,
                parameters,
                argumentConverters,
                resultConverter,
                invoker,
                method.getReturnType() != void.class
        );
    }

    private HandlerMethodArgumentConverter<HC>[] selectArgumentConverters(Method method, Parameter[] parameters) {
        @SuppressWarnings("unchecked")
        HandlerMethodArgumentConverter<HC>[] argumentConverters =
                (HandlerMethodArgumentConverter<HC>[]) new HandlerMethodArgumentConverter<?>[parameters.length];
        if (parameters.length == 0) {
            return argumentConverters;
        }
        if (argumentConverterSelector == null) {
            throw new IllegalStateException(
                    "HandlerMethodArgumentConverterSelector is required for handler method parameters: "
                            + method.toGenericString()
            );
        }
        for (int index = 0; index < parameters.length; index++) {
            argumentConverters[index] = argumentConverterSelector.select(method, parameters[index], index);
        }
        return argumentConverters;
    }

    private HandlerMethodResultConverter<HC> selectResultConverter(Method method) {
        if (method.getReturnType() == void.class) {
            return null;
        }
        if (resultConverterSelector == null) {
            throw new IllegalStateException(
                    "HandlerMethodResultConverterSelector is required for handler method result: "
                            + method.toGenericString()
            );
        }
        return resultConverterSelector.select(method);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void invokeWithConverters(HandlerMethodInvocationPlan plan, HandlerContext context) throws Exception {
        try {
            Object[] arguments = new Object[plan.parameters().length];
            for (int index = 0; index < arguments.length; index++) {
                HandlerMethodArgumentConverter converter = plan.argumentConverters()[index];
                arguments[index] = converter.convert(context, plan.method(), plan.parameters()[index], index);
            }

            Object result = null;
            if (plan.returnsValue()) {
                result = plan.invoker().invoke(arguments);
            } else {
                plan.invoker().invoke(arguments);
            }

            HandlerMethodResultConverter resultConverter = plan.resultConverter();
            if (resultConverter != null) {
                resultConverter.convert(context, plan.method(), result);
            }
        } catch (Throwable throwable) {
            if (throwable instanceof Exception exception) {
                throw exception;
            }
            if (throwable instanceof Error error) {
                throw error;
            }
            throw new RuntimeException(throwable);
        }
    }

    private List<HandlerInterceptor<HC>> createInterceptors(List<InterceptorAnnotationBinding> controllerInterceptorBinding, List<InterceptorAnnotationBinding> handlerInterceptorBinding) {
        List<HandlerInterceptor<HC>> interceptors = new ArrayList<>();
        appendInterceptors(interceptors, controllerInterceptorBinding);
        appendInterceptors(interceptors, handlerInterceptorBinding);
        return interceptors;
    }

    private void appendInterceptors(List<HandlerInterceptor<HC>> interceptors, List<InterceptorAnnotationBinding> bindings) {
        if (bindings.isEmpty()) {
            return;
        }
        if (annotationResolver == null) {
            throw new IllegalStateException("InterceptorAnnotationResolver is required when interceptor bindings are declared");
        }
        if (interceptorFactory == null) {
            throw new IllegalStateException("InterceptorFactory is required when interceptor bindings are declared");
        }
        for (InterceptorAnnotationBinding binding : bindings) {
            InterceptorConfigurationBinding configurationBinding = annotationResolver.resolve(binding.annotation(), binding.interceptorType());
            HandlerInterceptor<HC> interceptor = interceptorFactory.get(configurationBinding);
            if (interceptor == null) {
                throw new IllegalStateException("InterceptorFactory returned null for " + binding.interceptorType().getName());
            }
            interceptors.add(interceptor);
        }
    }

    private record HandlerMethodInvocationPlan<HC extends HandlerContext<?>>(
            Method method,
            Parameter[] parameters,
            HandlerMethodArgumentConverter<HC>[] argumentConverters,
            HandlerMethodResultConverter<HC> resultConverter,
            MethodHandle invoker,
            boolean returnsValue) {
    }

    private record InterceptorAnnotationBinding(Class<? extends HandlerInterceptor<?>> interceptorType,
                                                Annotation annotation,
                                                int order) {
    }
}
