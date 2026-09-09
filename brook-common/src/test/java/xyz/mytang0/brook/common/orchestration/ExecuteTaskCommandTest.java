package xyz.mytang0.brook.common.orchestration;

import org.junit.Assert;
import org.junit.Test;

public class ExecuteTaskCommandTest {

    @Test
    public void shouldRetainDeliveryIdentity() {
        ExecuteTaskCommand command = new ExecuteTaskCommand("flow-1", "task-1", 2);

        Assert.assertEquals("flow-1", command.getFlowId());
        Assert.assertEquals("task-1", command.getTaskId());
        Assert.assertEquals(2, command.getAttempt());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNegativeAttempt() {
        new ExecuteTaskCommand("flow-1", "task-1", -1);
    }
}
