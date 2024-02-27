package xyz.mytang0.brook.core.listener;

import xyz.mytang0.brook.common.context.FlowContext;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.spi.executor.ExecutorFactory;
import xyz.mytang0.brook.spi.listener.TaskListener;
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
