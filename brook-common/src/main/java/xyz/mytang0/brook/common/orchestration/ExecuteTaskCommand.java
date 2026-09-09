package xyz.mytang0.brook.common.orchestration;

import java.util.Objects;

public final class ExecuteTaskCommand implements FlowCommand {

    private static final long serialVersionUID = -4301636818407023507L;

    private final String flowId;

    private final String taskId;

    private final int attempt;

    public ExecuteTaskCommand(String flowId, String taskId, int attempt) {
        this.flowId = Objects.requireNonNull(flowId, "flowId");
        this.taskId = Objects.requireNonNull(taskId, "taskId");
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must not be negative");
        }
        this.attempt = attempt;
    }

    @Override
    public String getFlowId() {
        return flowId;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getAttempt() {
        return attempt;
    }
}
