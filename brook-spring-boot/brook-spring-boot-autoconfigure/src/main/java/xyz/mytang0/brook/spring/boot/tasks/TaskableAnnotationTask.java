package xyz.mytang0.brook.spring.boot.tasks;

import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.metadata.enums.FlowStatus;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.ExceptionUtils;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.common.utils.MethodUtils;
import xyz.mytang0.brook.core.FlowTaskRegistry;
import xyz.mytang0.brook.core.exception.FlowException;
import xyz.mytang0.brook.core.exception.TerminateException;
import xyz.mytang0.brook.core.utils.ThreadUtils;
import xyz.mytang0.brook.spi.task.FlowTask;
import xyz.mytang0.brook.spring.boot.annotation.Taskable;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static xyz.mytang0.brook.core.exception.FlowErrorCode.FLOW_EXECUTION_ERROR;

@Slf4j
@Configuration
public class TaskableAnnotationTask implements BeanPostProcessor {

    private final ExecutorService asyncRegister = Executors.newSingleThreadExecutor(
            ThreadUtils.threadsNamed("taskable-async-register-%d"));

    private final FlowTaskRegistry<FlowTask> flowTaskRegistry;

    TaskableAnnotationTask(FlowTaskRegistry<FlowTask> flowTaskRegistry) {
        this.flowTaskRegistry = flowTaskRegistry;
    }

    @Override
    public Object postProcessAfterInitialization(@Nonnull Object bean, @Nonnull String beanName) {

        asyncRegister.execute(() -> {
            try {
                register(bean);
            } catch (Exception e) {
                log.error(String.format(
                        "Register register taskable tasks of bean(name: %s, bean: %s) exception",
                        beanName, bean), e);
            }
        });

        return bean;
    }

    public void register(Object bean) {
        List<Method> methods =
                MethodUtils.getMethodsListWithAnnotation(AopUtils.getTargetClass(bean), Taskable.class);

        methods.forEach(method -> {

                    Taskable taskable = method.getAnnotation(Taskable.class);

                    if (flowTaskRegistry.getFlowTaskNoException(taskable.type()) != null) {
                        log.warn("Task type {} already exists!", taskable.type());
                        return;
                    }

                    flowTaskRegistry.register(new FlowTask() {

                        @Override
                        public ConfigOption<?> catalog() {
                            return ConfigOptions
                                    .key(taskable.type())
                                    .classType(Object.class)
                                    .asList()
                                    .noDefaultValue()
                                    .withDescription(taskable.description() +
                                            "(This task is invoked by a bean method, " +
                                            "so the input parameter is the input parameter of the method.)");
                        }

                        @Override
                        public void verify(@NotNull xyz.mytang0.brook.common.configuration.Configuration configuration) {
                            // Because of the characteristics of Java itself,
                            // the method name is compressed, so no verification is required.
                        }

                        @Override
                        public Object[] getInput(Object external) {
                            // No parameters.
                            if (method.getParameterCount() == 0) {
                                return null;
                            }

                            Object[] toBeConfirmed;

                            if (external != null && external.getClass().isArray()) {
                                toBeConfirmed =
                                        JsonUtils.convertValue(external, new TypeReference<Object[]>() {
                                        }.getType());
                            } else if (external instanceof List) {
                                toBeConfirmed = ((List<?>) external).toArray(new Object[0]);
                            } else {
                                toBeConfirmed = new Object[1];
                                toBeConfirmed[0] = external;
                            }

                            // Compatible with null input.
                            if (toBeConfirmed == null) {
                                toBeConfirmed = new Object[method.getParameterCount()];
                            }

                            if (method.getParameterCount() != toBeConfirmed.length) {
                                throw new IllegalArgumentException(
                                        String.format("Parameter mismatch, " +
                                                        "bean task %s has %d parameters, and the request passes %d parameters",
                                                getType(),
                                                method.getParameterCount(),
                                                toBeConfirmed.length));
                            }

                            Object[] args = new Object[method.getParameterCount()];
                            Class<?>[] parameterTypes = method.getParameterTypes();

                            for (int idx = 0; idx < method.getParameterCount(); idx++) {
                                args[idx] = JsonUtils.convertValue(toBeConfirmed[idx], parameterTypes[idx]);
                            }

                            return args;
                        }

                        @Override
                        public boolean execute(TaskInstance taskInstance) {

                            try {
                                Object output;

                                // Type has not been converted.
                                if (taskInstance.getInput() == null
                                        || !taskInstance.getInput().getClass().isArray()) {
                                    output = method.invoke(bean, getInput(taskInstance.getInput()));
                                } else {
                                    output = method.invoke(bean, taskInstance.getInput());
                                }

                                taskInstance.setOutput(output);
                            } catch (Throwable throwable) {

                                Throwable targetThrowable = throwable;

                                if (throwable instanceof InvocationTargetException) {
                                    targetThrowable = throwable.getCause();
                                }

                                if (taskable.isRetryable()) {
                                    throw new FlowException(FLOW_EXECUTION_ERROR, targetThrowable);
                                } else {
                                    log.error("Spring method invoke fail", targetThrowable);

                                    taskInstance.setStatus(TaskStatus.FAILED);
                                    taskInstance.setReasonForNotCompleting(
                                            ExceptionUtils.getMessage(targetThrowable));

                                    throw new TerminateException(
                                            taskInstance.getReasonForNotCompleting(),
                                            FlowStatus.FAILED,
                                            taskInstance);
                                }
                            }

                            taskInstance.setStatus(TaskStatus.COMPLETED);
                            return true;
                        }
                    });
                }
        );
    }
}
