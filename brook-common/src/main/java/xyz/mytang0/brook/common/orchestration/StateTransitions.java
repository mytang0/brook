package xyz.mytang0.brook.common.orchestration;

import xyz.mytang0.brook.common.metadata.enums.FlowStatus;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;

public final class StateTransitions {

    private StateTransitions() {
    }

    public static boolean isValid(FlowStatus current, FlowStatus next) {
        if (current == null || next == null) {
            return false;
        }
        if (current == next) {
            return true;
        }
        if (current.isTerminal()) {
            return false;
        }
        return current == FlowStatus.RUNNING
                || (current == FlowStatus.PAUSED && next == FlowStatus.RUNNING);
    }

    public static boolean isValid(TaskStatus current, TaskStatus next) {
        if (current == null || next == null) {
            return false;
        }
        if (current == next) {
            return true;
        }
        if (current.isTerminal() && !current.isHanged() && !current.isRetried()) {
            return false;
        }
        return current == TaskStatus.SCHEDULED
                || current == TaskStatus.IN_PROGRESS
                || current == TaskStatus.HANGED
                || current == TaskStatus.RETRIED;
    }
}
