package org.mytang.brook.spring.boot.tasks;


import org.mytang.brook.common.configuration.ConfigOption;
import org.mytang.brook.common.configuration.ConfigOptions;
import org.mytang.brook.common.configuration.Configuration;
import org.mytang.brook.common.metadata.enums.TaskStatus;
import org.mytang.brook.common.metadata.instance.TaskInstance;
import org.mytang.brook.common.utils.JsonUtils;
import org.mytang.brook.common.utils.MethodUtils;
import org.mytang.brook.core.exception.TerminateException;
import org.mytang.brook.spi.task.FlowTask;
import org.mytang.brook.spring.boot.utils.SpringContextUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    public boolean execute(TaskInstance taskInstance) {

        Configuration taskDefInput = taskInstance.getInputConfiguration();

        String beanName = taskDefInput.getString(Options.BEAN_NAME);
        String methodName = taskDefInput.getString(Options.METHOD_NAME);

        Object[] inputArgs = taskDefInput
                .getOptional(Options.ARGS)
                .map(values -> values.toArray(new Object[0]))
                .orElse(null);

        Object beanInstance;

        try {
            beanInstance = SpringContextUtils.getBean(beanName);
        } catch (NoSuchBeanDefinitionException e) {
            throw new IllegalArgumentException(
                    String.format("the %s bean does not exist", beanName));
        }

        Method method = MethodUtils.getAccessibleMethod(beanInstance.getClass(), methodName);
        if (method == null) {
            throw new IllegalArgumentException(
                    String.format("Method %s does not exist in %s bean", methodName, beanName));
        }

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
                .withDescription("Task input parameters, if the task has no input parameters, then pass in '[]'.");

        static final ConfigOption<String> METHOD_NAME = ConfigOptions
                .key("methodName")
                .stringType()
                .noDefaultValue()
                .withDescription("Task input parameters, if the task has no input parameters, then pass in '[]'.");

        static final ConfigOption<List<Object>> ARGS = ConfigOptions
                .key("args")
                .classType(Object.class)
                .asList()
                .noDefaultValue()
                .withDescription("The spring bean method input parameters, " +
                        "if the task has no input parameters, then pass in '[]'.");
    }
}
