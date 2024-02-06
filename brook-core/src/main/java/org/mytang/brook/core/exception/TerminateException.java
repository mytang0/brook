package org.mytang.brook.core.exception;

import org.mytang.brook.common.utils.ExceptionUtils;
import org.mytang.brook.common.metadata.enums.FlowStatus;
import org.mytang.brook.common.metadata.instance.TaskInstance;

public class TerminateException extends RuntimeException {

    private static final long serialVersionUID = -8630188712805899788L;

    private final FlowStatus flowStatus;

    private volatile TaskInstance taskInstance;

    public TerminateException(Throwable throwable) {
        this(ExceptionUtils.getMessage(throwable), FlowStatus.FAILED);
    }

    public TerminateException(String reason) {
        this(reason, FlowStatus.FAILED);
    }

    public TerminateException(String reason, FlowStatus flowStatus) {
        this(reason, flowStatus, null);
    }

    public TerminateException(String reason, TaskInstance taskInstance) {
        this(reason, null, taskInstance);
    }

    public TerminateException(String reason, FlowStatus flowStatus, TaskInstance taskInstance) {
        super(reason);
        this.flowStatus = flowStatus;
        this.taskInstance = taskInstance;
    }

    public FlowStatus getFlowStatus() {
        return flowStatus;
    }

    public TaskInstance getTaskInstance() {
        return taskInstance;
    }

    public void setTaskInstance(TaskInstance taskInstance) {
        if (this.taskInstance == null) {
            this.taskInstance = taskInstance;
        }
    }
}
