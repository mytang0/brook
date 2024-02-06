package org.mytang.brook.core.callback;

import org.mytang.brook.common.context.FlowContext;
import org.mytang.brook.common.extension.ExtensionDirector;
import org.mytang.brook.common.metadata.definition.TaskDef;
import org.mytang.brook.common.metadata.instance.FlowInstance;
import org.mytang.brook.common.metadata.instance.TaskInstance;
import org.mytang.brook.common.utils.StringUtils;
import org.mytang.brook.spi.callback.TaskCallback;
import org.mytang.brook.spi.executor.ExecutorFactory;

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
