package xyz.mytang0.brook.common.context;

import xyz.mytang0.brook.common.holder.UserHolder;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.metadata.model.User;

import java.util.Objects;

public abstract class FlowContext {

    private static final ThreadLocal<FlowInstance> currentFlow =
            ThreadLocal.withInitial(() -> null);

    private static final ThreadLocal<TaskInstance> currentTask =
            ThreadLocal.withInitial(() -> null);

    public static FlowInstance getCurrentFlow() {
        return currentFlow.get();
    }

    public static void setCurrentFlow(FlowInstance current) {
        User currentUser = current.getCreator();
        if (currentUser != null) {
            User oldUser = UserHolder.getCurrentUser();
            if (!Objects.equals(currentUser, oldUser)) {
                UserHolder.setCurrentUser(currentUser);
            }
        }
        currentFlow.set(current);
    }

    public static void removeCurrentFlow() {
        currentFlow.remove();
        UserHolder.clearCurrentUser();
    }

    public static TaskInstance getCurrentTask() {
        return currentTask.get();
    }

    public static void setCurrentTask(TaskInstance current) {
        currentTask.set(current);
    }

    public static void removeCurrentTask() {
        currentTask.remove();
    }
}
