package xyz.mytang0.brook.common.orchestration;

import xyz.mytang0.brook.common.metadata.enums.FlowStatus;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import org.junit.Assert;
import org.junit.Test;

public class StateTransitionsTest {

    @Test
    public void shouldAllowLegalFlowTransitions() {
        Assert.assertTrue(StateTransitions.isValid(
                FlowStatus.RUNNING, FlowStatus.COMPLETED));
        Assert.assertTrue(StateTransitions.isValid(
                FlowStatus.PAUSED, FlowStatus.RUNNING));
    }

    @Test
    public void shouldRejectTerminalFlowTransition() {
        Assert.assertFalse(StateTransitions.isValid(
                FlowStatus.COMPLETED, FlowStatus.RUNNING));
    }

    @Test
    public void shouldAllowHangTaskCompletion() {
        Assert.assertTrue(StateTransitions.isValid(
                TaskStatus.HANGED, TaskStatus.COMPLETED));
    }

    @Test
    public void shouldRejectTerminalTaskTransition() {
        Assert.assertFalse(StateTransitions.isValid(
                TaskStatus.COMPLETED, TaskStatus.IN_PROGRESS));
    }
}
