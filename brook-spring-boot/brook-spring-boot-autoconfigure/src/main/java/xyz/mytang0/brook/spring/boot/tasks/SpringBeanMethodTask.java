package xyz.mytang0.brook.spring.boot.tasks;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.common.utils.MethodUtils;
import xyz.mytang0.brook.core.exception.TerminateException;
import xyz.mytang0.brook.spi.task.FlowTask;
import xyz.mytang0.brook.spring.boot.utils.SpringContextUtils;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SpringBeanMethodTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("springBeanMethod")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Call the Spring bean method of the flow engine deployment application.");

    @Override
    public ConfigOption<?> catalog() {
        return CATALOG;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> required = new HashSet<>();
        required.add(Options.BEAN_NAME);
        required.add(Options.METHOD_NAME);
        required.add(Options.ARGS);
        return required;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optional = new HashSet<>();
        optional.add(Options.TYPES);
        return optional;
    }

    @Override
    public boolean execute(TaskInstance taskInstance) {

        Configuration taskDefInput = taskInstance.getInputConfiguration();

        String beanName = taskDefInput.getString(Options.BEAN_NAME);
        String methodName = taskDefInput.getString(Options.METHOD_NAME);

        Object beanInstance;

        try {
            beanInstance = SpringContextUtils.getBean(beanName);
        } catch (NoSuchBeanDefinitionException e) {
            throw new IllegalArgumentException(
                    String.format("the %s bean does not exist", beanName));
        }

        Class<?>[] types = taskDefInput.getOptional(Options.TYPES)
                .map(values ->
                        values.stream().map(type -> {
                                    try {
                                        return Class.forName(type);
                                    } catch (ClassNotFoundException e) {
                                        throw new IllegalArgumentException(
                                                String.format("Parameter type [%s] does not exist", type));
                                    }
                                })
                                .collect(Collectors.toList())
                                .toArray(new Class<?>[0])
                )
                .orElse(null);

        Method method = types != null
                ? MethodUtils.getAccessibleMethod(beanInstance.getClass(), methodName, types)
                : MethodUtils.getAccessibleMethod(beanInstance.getClass(), methodName);
        if (method == null) {
            throw new IllegalArgumentException(
                    String.format("Method %s does not exist in %s bean", methodName, beanName));
        }

        Object[] inputArgs = taskDefInput.getOptional(Options.ARGS)
                .map(values -> values.toArray(new Object[0]))
                .orElse(null);

        if (inputArgs == null ||
                method.getParameterCount() != inputArgs.length) {
            throw new IllegalArgumentException(
                    String.format("Parameter mismatch, " +
                                    "bean %s's method %s has %d parameters, and the request passes %d parameters",
                            beanName,
                            methodName,
                            method.getParameterCount(),
                            Optional.ofNullable(inputArgs)
                                    .map(__ -> __.length).orElse(0)));
        }

        Object[] args = new Object[method.getParameterCount()];

        if (0 < method.getParameterCount()) {
            Class<?>[] parameterTypes = method.getParameterTypes();

            for (int idx = 0; idx < method.getParameterCount(); idx++) {
                args[idx] = JsonUtils.convertValue(inputArgs[idx], parameterTypes[idx]);
            }
        }

        try {
            Object result = method.invoke(beanInstance, args);
            taskInstance.setOutput(result);
        } catch (Throwable throwable) {
            log.error("spring method invoke fail", throwable);
            throw new TerminateException(throwable.getLocalizedMessage(), taskInstance);
        }

        taskInstance.setStatus(TaskStatus.COMPLETED);
        return true;
    }

    static class Options {

        static final ConfigOption<String> BEAN_NAME = ConfigOptions
                .key("beanName")
                .stringType()
                .noDefaultValue()
                .withDescription("The Spring Bean name.");

        static final ConfigOption<String> METHOD_NAME = ConfigOptions
                .key("methodName")
                .stringType()
                .noDefaultValue()
                .withDescription("The Spring Bean method name.");

        static final ConfigOption<List<String>> TYPES = ConfigOptions
                .key("types")
                .classType(String.class)
                .asList()
                .defaultValues()
                .withDescription("The Spring Bean method  parameter types, " +
                        "When a method with the same name exists in the Bean, the parameter type must be specified.");

        static final ConfigOption<List<Object>> ARGS = ConfigOptions
                .key("args")
                .classType(Object.class)
                .asList()
                .noDefaultValue()
                .withDescription("The Spring Bean method input parameters, " +
                        "if the task has no input parameters, then pass in '[]'.");
    }
}
