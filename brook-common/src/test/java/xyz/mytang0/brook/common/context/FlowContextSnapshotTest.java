package xyz.mytang0.brook.common.context;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import xyz.mytang0.brook.common.holder.UserHolder;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.metadata.model.User;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class FlowContextSnapshotTest {

    @Before
    public void setUp() {
        FlowContext.removeCurrentFlow();
        FlowContext.removeCurrentTask();
        UserHolder.clearCurrentUser();
    }

    @After
    public void tearDown() {
        FlowContext.removeCurrentFlow();
        FlowContext.removeCurrentTask();
        UserHolder.clearCurrentUser();
    }

    @Test
    public void testCaptureEmpty() {
        FlowContextSnapshot snapshot = FlowContextSnapshot.capture();
        Assert.assertNotNull(snapshot);
        Assert.assertNull(snapshot.getFlowInstance());
        Assert.assertNull(snapshot.getTaskInstance());
        Assert.assertNull(snapshot.getUser());
    }

    @Test
    public void testCaptureWithFlowAndTask() {
        FlowInstance flow = new FlowInstance();
        flow.setFlowId("flow-1");
        User user = new User();
        flow.setCreator(user);
        FlowContext.setCurrentFlow(flow);

        TaskInstance task = new TaskInstance();
        task.setTaskId("task-1");
        FlowContext.setCurrentTask(task);

        FlowContextSnapshot snapshot = FlowContextSnapshot.capture();

        Assert.assertSame(flow, snapshot.getFlowInstance());
        Assert.assertSame(task, snapshot.getTaskInstance());
        Assert.assertSame(user, snapshot.getUser());
    }

    @Test
    public void testApplyRestoresContext() {
        FlowInstance flow = new FlowInstance();
        flow.setFlowId("flow-1");
        User user = new User();
        flow.setCreator(user);
        FlowContext.setCurrentFlow(flow);

        TaskInstance task = new TaskInstance();
        task.setTaskId("task-1");
        FlowContext.setCurrentTask(task);

        FlowContextSnapshot snapshot = FlowContextSnapshot.capture();

        // Clear context
        FlowContext.removeCurrentFlow();
        FlowContext.removeCurrentTask();

        Assert.assertNull(FlowContext.getCurrentFlow());
        Assert.assertNull(FlowContext.getCurrentTask());

        // Apply snapshot
        snapshot.apply();

        Assert.assertSame(flow, FlowContext.getCurrentFlow());
        Assert.assertSame(task, FlowContext.getCurrentTask());
    }

    @Test
    public void testClearRemovesContext() {
        FlowInstance flow = new FlowInstance();
        flow.setFlowId("flow-1");
        User user = new User();
        flow.setCreator(user);
        FlowContext.setCurrentFlow(flow);

        FlowContextSnapshot snapshot = FlowContextSnapshot.capture();

        Assert.assertNotNull(FlowContext.getCurrentFlow());

        snapshot.clear();

        Assert.assertNull(FlowContext.getCurrentFlow());
        Assert.assertNull(FlowContext.getCurrentTask());
    }

    @Test
    public void testPropagationAcrossThreads() throws Exception {
        FlowInstance flow = new FlowInstance();
        flow.setFlowId("flow-propagation");
        User user = new User();
        flow.setCreator(user);
        FlowContext.setCurrentFlow(flow);

        TaskInstance task = new TaskInstance();
        task.setTaskId("task-propagation");
        FlowContext.setCurrentTask(task);

        FlowContextSnapshot snapshot = FlowContextSnapshot.capture();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<FlowInstance> capturedFlow = new AtomicReference<>();
        AtomicReference<TaskInstance> capturedTask = new AtomicReference<>();

        executor.submit(() -> {
            // Before apply, context should be empty on this thread
            Assert.assertNull(FlowContext.getCurrentFlow());
            Assert.assertNull(FlowContext.getCurrentTask());

            snapshot.apply();
            try {
                capturedFlow.set(FlowContext.getCurrentFlow());
                capturedTask.set(FlowContext.getCurrentTask());
            } finally {
                snapshot.clear();
                latch.countDown();
            }
        });

        latch.await();
        executor.shutdown();

        Assert.assertSame(flow, capturedFlow.get());
        Assert.assertSame(task, capturedTask.get());

        // Original thread's context should be unaffected
        Assert.assertSame(flow, FlowContext.getCurrentFlow());
        Assert.assertSame(task, FlowContext.getCurrentTask());
    }

    @Test
    public void testPropagatingRunnableWrapper() throws Exception {
        FlowInstance flow = new FlowInstance();
        flow.setFlowId("flow-wrapper");
        User user = new User();
        flow.setCreator(user);
        FlowContext.setCurrentFlow(flow);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<FlowInstance> capturedFlow = new AtomicReference<>();

        executor.submit(FlowContextPropagatingRunnable.wrap(() -> {
            capturedFlow.set(FlowContext.getCurrentFlow());
            latch.countDown();
        }));

        latch.await();
        executor.shutdown();

        Assert.assertSame(flow, capturedFlow.get());
    }

    @Test
    public void testPropagatingCallableWrapper() throws Exception {
        FlowInstance flow = new FlowInstance();
        flow.setFlowId("flow-callable");
        User user = new User();
        flow.setCreator(user);
        FlowContext.setCurrentFlow(flow);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        FlowInstance result = executor.submit(
                FlowContextPropagatingRunnable.<FlowInstance>wrap(
                        FlowContext::getCurrentFlow)
        ).get();

        executor.shutdown();

        Assert.assertSame(flow, result);
    }

    @Test
    public void testContextIsolationBetweenThreads() throws Exception {
        FlowInstance flow1 = new FlowInstance();
        flow1.setFlowId("flow-1");
        User user1 = new User();
        flow1.setCreator(user1);

        FlowInstance flow2 = new FlowInstance();
        flow2.setFlowId("flow-2");
        User user2 = new User();
        flow2.setCreator(user2);

        // Set up snapshot from flow1
        FlowContext.setCurrentFlow(flow1);
        FlowContextSnapshot snapshot1 = FlowContextSnapshot.capture();

        // Switch to flow2
        FlowContext.setCurrentFlow(flow2);
        FlowContextSnapshot snapshot2 = FlowContextSnapshot.capture();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<FlowInstance> thread1Flow = new AtomicReference<>();
        AtomicReference<FlowInstance> thread2Flow = new AtomicReference<>();

        executor.submit(() -> {
            snapshot1.apply();
            try {
                thread1Flow.set(FlowContext.getCurrentFlow());
            } finally {
                snapshot1.clear();
                latch.countDown();
            }
        });

        executor.submit(() -> {
            snapshot2.apply();
            try {
                thread2Flow.set(FlowContext.getCurrentFlow());
            } finally {
                snapshot2.clear();
                latch.countDown();
            }
        });

        latch.await();
        executor.shutdown();

        Assert.assertSame(flow1, thread1Flow.get());
        Assert.assertSame(flow2, thread2Flow.get());
    }
}
