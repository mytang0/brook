package xyz.mytang0.brook.core.aspect;

import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.core.callback.TaskCallbackFacade;
import xyz.mytang0.brook.core.listener.TaskListenerFacade;

public final class TaskAspect {

    private final TaskCallbackFacade taskCallbackFacade;

    private final TaskListenerFacade taskListenerFacade;


    public TaskAspect() {
        this.taskCallbackFacade = new TaskCallbackFacade();
        this.taskListenerFacade = new TaskListenerFacade();
    }

    public void onCreated(final TaskInstance taskInstance) {
        if (taskInstance.getRetryCount() > 0) {
            return;
        }
        taskCallbackFacade.onCreated(taskInstance);
        taskListenerFacade.onCreated(taskInstance);
    }

    public void onTerminated(final TaskInstance taskInstance) {
        TaskStatus status = taskInstance.getStatus();
        if (status.isTerminal()
                && !status.isHanged()
                && !status.isRetried()
                && !taskInstance.isExecuted()) {
            taskCallbackFacade.onTerminated(taskInstance);
            taskListenerFacade.onTerminated(taskInstance);
        }
    }
}
