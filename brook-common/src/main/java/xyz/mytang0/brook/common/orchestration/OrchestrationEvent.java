package xyz.mytang0.brook.common.orchestration;

import xyz.mytang0.brook.common.metadata.enums.FlowStatus;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;

import java.io.Serializable;
import java.util.Objects;

public final class OrchestrationEvent implements Serializable {

    private static final long serialVersionUID = 7691233651398803101L;

    public enum Type {
        FLOW_CREATED,
        FLOW_TERMINATED,
        TASK_SCHEDULED,
        TASK_STARTED,
        TASK_TERMINATED
    }

    private final Type type;

    private final String flowId;

    private final String taskId;

    private final int attempt;

    private final FlowStatus flowStatus;

    private final TaskStatus taskStatus;

    public OrchestrationEvent(Type type, String flowId, String taskId, int attempt,
                              FlowStatus flowStatus, TaskStatus taskStatus) {
        this.type = Objects.requireNonNull(type, "type");
        this.flowId = Objects.requireNonNull(flowId, "flowId");
        this.taskId = taskId;
        this.attempt = attempt;
        this.flowStatus = flowStatus;
        this.taskStatus = taskStatus;
    }

    public Type getType() {
        return type;
    }

    public String getFlowId() {
        return flowId;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getAttempt() {
        return attempt;
    }

    public FlowStatus getFlowStatus() {
        return flowStatus;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }
}
