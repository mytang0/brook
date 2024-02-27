package xyz.mytang0.brook.core.callback;

import xyz.mytang0.brook.common.context.FlowContext;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.StringUtils;
import xyz.mytang0.brook.spi.callback.TaskCallback;
import xyz.mytang0.brook.spi.executor.ExecutorFactory;

import java.util.concurrent.ExecutorService;

public class TaskCallbackFacade {

    private final ExecutorService asyncExecutor;

    public TaskCallbackFacade() {
        this.asyncExecutor = ExtensionDirector
                .getExtensionLoader(ExecutorFactory.class)
                .getDefaultExtension()
                .getExecutor(this.getClass().getSimpleName());
    }

    public void onCreated(TaskInstance taskInstance) {
        TaskDef taskDef = taskInstance.getTaskDef();
        if (taskDef == null) {
            return;
        }

        TaskDef.Callback callbackDef = taskDef.getCallback();
        if (callbackDef == null) {
            return;
        }

        final TaskCallback taskCallback =
                getTaskCallback(callbackDef.getProtocol());

        if (callbackDef.isAsync()) {
            final FlowInstance currentFlow = FlowContext.getCurrentFlow();
            asyncExecutor.execute(() -> {
                try {
                    FlowContext.setCurrentFlow(currentFlow);
                    taskCallback.onCreated(callbackDef.getInput(), taskInstance);
                } finally {
                    FlowContext.removeCurrentFlow();
                }
            });
        } else {
            taskCallback.onCreated(callbackDef.getInput(), taskInstance);
        }
    }

    public void onTerminated(TaskInstance taskInstance) {
        TaskDef taskDef = taskInstance.getTaskDef();
        if (taskDef == null) {
            return;
        }

        TaskDef.Callback callbackDef = taskDef.getCallback();
        if (callbackDef == null) {
            return;
        }

        final TaskCallback taskCallback =
                getTaskCallback(callbackDef.getProtocol());

        if (callbackDef.isAsync()) {
            final FlowInstance currentFlow = FlowContext.getCurrentFlow();
            asyncExecutor.execute(() -> {
                try {
                    FlowContext.setCurrentFlow(currentFlow);
                    taskCallback.onTerminated(callbackDef.getInput(), taskInstance);
                } finally {
                    FlowContext.removeCurrentFlow();
                }
            });
        } else {
            taskCallback.onTerminated(callbackDef.getInput(), taskInstance);
        }
    }

    private TaskCallback getTaskCallback(String protocol) {
        return StringUtils.isBlank(protocol)
                ? ExtensionDirector.getExtensionLoader(TaskCallback.class).getDefaultExtension()
                : ExtensionDirector.getExtensionLoader(TaskCallback.class).getExtension(protocol);
    }
}
