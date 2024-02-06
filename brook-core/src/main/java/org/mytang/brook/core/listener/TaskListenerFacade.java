package org.mytang.brook.core.listener;

import org.mytang.brook.common.context.FlowContext;
import org.mytang.brook.common.extension.ExtensionDirector;
import org.mytang.brook.common.extension.ExtensionLoader;
import org.mytang.brook.common.metadata.instance.FlowInstance;
import org.mytang.brook.common.metadata.instance.TaskInstance;
import org.mytang.brook.spi.executor.ExecutorFactory;
import org.mytang.brook.spi.listener.TaskListener;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

@Slf4j
public class TaskListenerFacade {

    private final ExecutorService asyncExecutor;

    private final ExtensionLoader<TaskListener> taskListenerLoader;

    public TaskListenerFacade() {
        this.asyncExecutor = ExtensionDirector
                .getExtensionLoader(ExecutorFactory.class)
                .getDefaultExtension()
                .getExecutor(this.getClass().getSimpleName());
        this.taskListenerLoader = ExtensionDirector
                .getExtensionLoader(TaskListener.class);
    }

    public void onCreated(TaskInstance taskInstance) {
        final FlowInstance currentFlow = FlowContext.getCurrentFlow();
        taskListenerLoader.getExtensionInstances().forEach(taskListener -> {
            if (taskListener.test(taskInstance)) {
                if (taskListener.isAsync()) {
                    asyncExecutor.execute(() -> {
                        try {
                            Optional.ofNullable(currentFlow)
                                    .ifPresent(FlowContext::setCurrentFlow);
                            taskListener.onCreated(taskInstance);
                        } catch (Throwable throwable) {
                            log.error(String.format("Async execute %s taskId(%s) onCreated exception",
                                            taskInstance.getClass().getSimpleName(),
                                            taskInstance.getFlowId()),
                                    throwable);
                        } finally {
                            FlowContext.removeCurrentFlow();
                        }
                    });
                } else {
                    taskListener.onCreated(taskInstance);
                }
            }
        });
    }

    public void onTerminated(TaskInstance taskInstance) {
        final FlowInstance currentFlow = FlowContext.getCurrentFlow();
        taskListenerLoader.getExtensionInstances().forEach(taskListener -> {
            if (taskListener.test(taskInstance)) {
                if (taskListener.isAsync()) {
                    asyncExecutor.execute(() -> {
                        try {
                            Optional.ofNullable(currentFlow)
                                    .ifPresent(FlowContext::setCurrentFlow);
                            taskListener.onTerminated(taskInstance);
                        } catch (Throwable throwable) {
                            log.error(String.format("Async execute %s taskId(%s) onTerminated exception",
                                            taskInstance.getClass().getSimpleName(),
                                            taskInstance.getFlowId()),
                                    throwable);
                        } finally {
                            FlowContext.removeCurrentFlow();
                        }
                    });
                } else {
                    taskListener.onTerminated(taskInstance);
                }
            }
        });
    }
}
