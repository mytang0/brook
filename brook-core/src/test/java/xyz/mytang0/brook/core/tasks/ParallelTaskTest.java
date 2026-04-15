package xyz.mytang0.brook.core.tasks;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import xyz.mytang0.brook.common.constants.TaskConstants;
import xyz.mytang0.brook.common.context.FlowContext;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.context.TaskMapperContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParallelTaskTest {

    private ParallelTask parallelTask;

    @Before
    public void setUp() {
        parallelTask = new ParallelTask();
    }

    @Test
    public void testCatalogKey() {
        Assert.assertEquals("PARALLEL", parallelTask.catalog().key());
    }

    @Test
    public void testGetType() {
        Assert.assertEquals("PARALLEL", parallelTask.getType());
    }

    @Test
    public void testRequiredOptions() {
        Assert.assertFalse(parallelTask.requiredOptions().isEmpty());
    }

    @Test
    public void testOptionalOptions() {
        Assert.assertFalse(parallelTask.optionalOptions().isEmpty());
    }

    @Test
    public void testExecute() {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setStatus(TaskStatus.SCHEDULED);

        boolean result = parallelTask.execute(taskInstance);

        Assert.assertTrue(result);
        Assert.assertEquals(TaskStatus.COMPLETED, taskInstance.getStatus());
    }

    @Test
    public void testExtractBranchIndex() {
        Assert.assertEquals(0, ParallelTask.extractBranchIndex(
                "task1" + TaskConstants.PARALLEL_INDEX_SEPARATOR + "0"));
        Assert.assertEquals(5, ParallelTask.extractBranchIndex(
                "task1" + TaskConstants.PARALLEL_INDEX_SEPARATOR + "5"));
        Assert.assertEquals(-1, ParallelTask.extractBranchIndex("task1"));
        Assert.assertEquals(-1, ParallelTask.extractBranchIndex(
                "task1" + TaskConstants.PARALLEL_INDEX_SEPARATOR + "abc"));
    }

    @Test
    public void testGetMappedTasksReturnsSingleInstance() {
        TaskDef taskDef = createParallelTaskDef();
        FlowInstance flowInstance = new FlowInstance();
        flowInstance.setFlowId("flow-1");
        flowInstance.setTaskInstances(new ArrayList<>());

        TaskMapperContext context = TaskMapperContext.builder()
                .taskDef(taskDef)
                .flowInstance(flowInstance)
                .input(taskDef.getInput())
                .build();

        List<TaskInstance> mappedTasks = parallelTask.getMappedTasks(context);

        Assert.assertEquals(1, mappedTasks.size());
        TaskInstance parallelTaskInstance = mappedTasks.get(0);
        Assert.assertEquals("flow-1", parallelTaskInstance.getFlowId());
        Assert.assertNotNull(parallelTaskInstance.getOutput());
    }

    @Test
    public void testGetBranches() {
        TaskDef taskDef = createParallelTaskDef();
        List<ParallelTask.Branch> branches = parallelTask.getBranches(taskDef);

        Assert.assertNotNull(branches);
        Assert.assertEquals(2, branches.size());
        Assert.assertEquals("branchA", branches.get(0).getName());
        Assert.assertEquals("branchB", branches.get(1).getName());
        Assert.assertEquals(1, branches.get(0).getTasks().size());
        Assert.assertEquals(1, branches.get(1).getTasks().size());
    }

    @Test
    public void testGetBranchesCachesResult() {
        TaskDef taskDef = createParallelTaskDef();

        // First call parses
        List<ParallelTask.Branch> branches1 = parallelTask.getBranches(taskDef);
        // Second call should use cache (taskDef.parsed)
        List<ParallelTask.Branch> branches2 = parallelTask.getBranches(taskDef);

        Assert.assertSame(branches1, branches2);
    }

    @Test
    public void testGetBranchEntryTasks() {
        TaskDef taskDef = createParallelTaskDef();

        List<TaskDef> entryTasks = parallelTask.getBranchEntryTasks(taskDef);

        Assert.assertEquals(2, entryTasks.size());
        Assert.assertTrue(entryTasks.get(0).getName().contains(
                TaskConstants.PARALLEL_INDEX_SEPARATOR + "0"));
        Assert.assertTrue(entryTasks.get(1).getName().contains(
                TaskConstants.PARALLEL_INDEX_SEPARATOR + "1"));
    }

    @Test
    public void testGetFailurePolicyDefault() {
        TaskDef taskDef = createParallelTaskDef();
        ParallelTask.FailurePolicy policy = parallelTask.getFailurePolicy(taskDef);
        Assert.assertEquals(ParallelTask.FailurePolicy.FAIL_FAST, policy);
    }

    @Test
    public void testGetFailurePolicyWaitAll() {
        TaskDef taskDef = createParallelTaskDefWithPolicy("WAIT_ALL");
        ParallelTask.FailurePolicy policy = parallelTask.getFailurePolicy(taskDef);
        Assert.assertEquals(ParallelTask.FailurePolicy.WAIT_ALL, policy);
    }

    @Test
    public void testGetFailurePolicyInvalid() {
        TaskDef taskDef = createParallelTaskDefWithPolicy("INVALID_POLICY");
        ParallelTask.FailurePolicy policy = parallelTask.getFailurePolicy(taskDef);
        // Invalid policy should fall back to FAIL_FAST
        Assert.assertEquals(ParallelTask.FailurePolicy.FAIL_FAST, policy);
    }

    @Test
    public void testBranchTaskNameUniqueness() {
        TaskDef taskDef = createParallelTaskDefWithSameChildNames();

        List<TaskDef> entryTasks = parallelTask.getBranchEntryTasks(taskDef);

        Assert.assertEquals(2, entryTasks.size());
        // Even with same child names, branch suffixes ensure uniqueness
        Assert.assertNotEquals(
                entryTasks.get(0).getName(),
                entryTasks.get(1).getName());
    }

    @Test
    public void testCancel() {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setStatus(TaskStatus.IN_PROGRESS);

        parallelTask.cancel(taskInstance);

        Assert.assertEquals(TaskStatus.CANCELED, taskInstance.getStatus());
    }

    @Test
    public void testCancelAlreadyTerminal() {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setStatus(TaskStatus.COMPLETED);

        parallelTask.cancel(taskInstance);

        // Should not change status if already terminal
        Assert.assertEquals(TaskStatus.COMPLETED, taskInstance.getStatus());
    }

    @Test
    public void testNextWithSelfReference() {
        TaskDef taskDef = createParallelTaskDef();

        // Set up flow context for next() method
        FlowInstance flowInstance = new FlowInstance();
        flowInstance.setFlowId("flow-1");
        flowInstance.setTaskInstances(new ArrayList<>());

        TaskInstance parallelTaskInstance = new TaskInstance();
        parallelTaskInstance.setTaskId("parallel-1");
        parallelTaskInstance.setTaskName("parallelTask");
        parallelTaskInstance.setTaskDef(taskDef);
        Map<String, Object> output = new HashMap<>();
        output.put("branchOutputs", new HashMap<>());
        parallelTaskInstance.setOutput(output);
        flowInstance.getTaskInstances().add(parallelTaskInstance);

        FlowContext.setCurrentFlow(flowInstance);

        try {
            TaskDef result = parallelTask.next(taskDef, taskDef);
            Assert.assertNotNull(result);
            // Should return the first task of the first branch with suffix
            Assert.assertTrue(result.getName().contains(
                    TaskConstants.PARALLEL_INDEX_SEPARATOR + "0"));
        } finally {
            FlowContext.removeCurrentFlow();
        }
    }

    // Helper methods

    private TaskDef createParallelTaskDef() {
        TaskDef taskDef = new TaskDef();
        taskDef.setName("parallelTask");
        taskDef.setType("PARALLEL");

        Map<String, Object> input = new HashMap<>();
        List<Map<String, Object>> branches = new ArrayList<>();

        Map<String, Object> branchA = new HashMap<>();
        branchA.put("name", "branchA");
        List<Map<String, Object>> tasksA = new ArrayList<>();
        Map<String, Object> taskA1 = new HashMap<>();
        taskA1.put("name", "taskA1");
        taskA1.put("type", "COMPUTING");
        taskA1.put("input", new HashMap<>());
        tasksA.add(taskA1);
        branchA.put("tasks", tasksA);
        branches.add(branchA);

        Map<String, Object> branchB = new HashMap<>();
        branchB.put("name", "branchB");
        List<Map<String, Object>> tasksB = new ArrayList<>();
        Map<String, Object> taskB1 = new HashMap<>();
        taskB1.put("name", "taskB1");
        taskB1.put("type", "COMPUTING");
        taskB1.put("input", new HashMap<>());
        tasksB.add(taskB1);
        branchB.put("tasks", tasksB);
        branches.add(branchB);

        input.put("branches", branches);
        taskDef.setInput(input);

        return taskDef;
    }

    private TaskDef createParallelTaskDefWithPolicy(String policy) {
        TaskDef taskDef = createParallelTaskDef();
        @SuppressWarnings("unchecked")
        Map<String, Object> input = (Map<String, Object>) taskDef.getInput();
        input.put("failurePolicy", policy);
        return taskDef;
    }

    private TaskDef createParallelTaskDefWithSameChildNames() {
        TaskDef taskDef = new TaskDef();
        taskDef.setName("parallelTask");
        taskDef.setType("PARALLEL");

        Map<String, Object> input = new HashMap<>();
        List<Map<String, Object>> branches = new ArrayList<>();

        Map<String, Object> branchA = new HashMap<>();
        branchA.put("name", "branchA");
        List<Map<String, Object>> tasksA = new ArrayList<>();
        Map<String, Object> taskA1 = new HashMap<>();
        taskA1.put("name", "commonTask");
        taskA1.put("type", "COMPUTING");
        taskA1.put("input", new HashMap<>());
        tasksA.add(taskA1);
        branchA.put("tasks", tasksA);
        branches.add(branchA);

        Map<String, Object> branchB = new HashMap<>();
        branchB.put("name", "branchB");
        List<Map<String, Object>> tasksB = new ArrayList<>();
        Map<String, Object> taskB1 = new HashMap<>();
        taskB1.put("name", "commonTask");
        taskB1.put("type", "COMPUTING");
        taskB1.put("input", new HashMap<>());
        tasksB.add(taskB1);
        branchB.put("tasks", tasksB);
        branches.add(branchB);

        input.put("branches", branches);
        taskDef.setInput(input);

        return taskDef;
    }
}
