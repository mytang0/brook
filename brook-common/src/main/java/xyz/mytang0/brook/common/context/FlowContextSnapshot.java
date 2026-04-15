package xyz.mytang0.brook.common.context;

import xyz.mytang0.brook.common.holder.UserHolder;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.metadata.model.User;

/**
 * Immutable snapshot of the current thread's FlowContext and UserHolder state.
 * <p>
 * Used to propagate context across threads when executing parallel branches.
 * Captures the current ThreadLocal state at creation time, and can be applied
 * to a different thread before execution.
 */
public final class FlowContextSnapshot {

    private final FlowInstance flowInstance;

    private final TaskInstance taskInstance;

    private final User user;

    private FlowContextSnapshot(FlowInstance flowInstance,
                                TaskInstance taskInstance,
                                User user) {
        this.flowInstance = flowInstance;
        this.taskInstance = taskInstance;
        this.user = user;
    }

    /**
     * Captures the current thread's FlowContext and UserHolder state.
     *
     * @return a snapshot of the current context
     */
    public static FlowContextSnapshot capture() {
        return new FlowContextSnapshot(
                FlowContext.getCurrentFlow(),
                FlowContext.getCurrentTask(),
                UserHolder.getCurrentUser()
        );
    }

    /**
     * Applies this snapshot to the current thread's ThreadLocals.
     * <p>
     * Should be called at the start of a parallel branch execution
     * to restore the context captured from the parent thread.
     */
    public void apply() {
        if (flowInstance != null) {
            FlowContext.setCurrentFlow(flowInstance);
        }
        if (taskInstance != null) {
            FlowContext.setCurrentTask(taskInstance);
        }
        if (user != null) {
            UserHolder.setCurrentUser(user);
        }
    }

    /**
     * Clears the current thread's FlowContext and UserHolder state.
     * <p>
     * Should be called in a finally block after parallel branch execution
     * completes to prevent ThreadLocal leaks.
     */
    public void clear() {
        FlowContext.removeCurrentFlow();
        FlowContext.removeCurrentTask();
        UserHolder.clearCurrentUser();
    }

    public FlowInstance getFlowInstance() {
        return flowInstance;
    }

    public TaskInstance getTaskInstance() {
        return taskInstance;
    }

    public User getUser() {
        return user;
    }
}
