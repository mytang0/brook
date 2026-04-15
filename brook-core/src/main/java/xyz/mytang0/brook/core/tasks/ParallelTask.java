package xyz.mytang0.brook.core.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.context.FlowContext;
import xyz.mytang0.brook.common.context.TaskMapperContext;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.core.FlowExecutor;
import xyz.mytang0.brook.spi.task.FlowTask;

import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Parallel task, used to execute multiple branches concurrently.
 * <p>
 * Each branch contains one or more sequential TaskDefs. The PARALLEL task
 * creates one TaskInstance per branch entry task during {@link #getMappedTasks(TaskMapperContext)},
 * linking them via {@code parentTaskId}/{@code subTaskIds} to the PARALLEL task itself.
 * <p>
 * Branch entry tasks are scheduled through the normal {@code decide()} loop.
 * The PARALLEL task monitors its children: when all branch tasks reach a terminal
 * state, the PARALLEL task itself completes. This design ensures that each branch
 * task can run on any node/instance independently.
 * <p>
 * Unlike LoopTask, ParallelTask does <em>not</em> add a suffix to child task names.
 * The uniqueness of task names is guaranteed before the process begins, so there is
 * no need to forcibly rename tasks, which would add unnecessary complexity and
 * performance overhead.
 * <p>
 * Input schema:
 * <pre>
 * {
 *   "branches": [
 *     { "name": "branchA", "tasks": [TaskDef, TaskDef, ...] },
 *     { "name": "branchB", "tasks": [TaskDef, TaskDef, ...] }
 *   ],
 *   "failurePolicy": "FAIL_FAST" | "WAIT_ALL"  // optional, default FAIL_FAST
 * }
 * </pre>
 */
@Slf4j
public class ParallelTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("PARALLEL")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Parallel task, executes multiple branches concurrently.");

    // Output keys.
    static final String BRANCH_OUTPUTS_KEY = "branchOutputs";

    static final String FAILED_BRANCH_KEY = "failedBranch";

    @Setter
    private FlowExecutor<?> flowExecutor;

    @Override
    public ConfigOption<?> catalog() {
        return CATALOG;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(Options.BRANCHES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(Options.FAILURE_POLICY);
        return options;
    }

    @Override
    public void doVerify(@NotNull Configuration configuration) {
        List<Branch> branches = configuration.get(Options.BRANCHES);
        if (branches == null || branches.isEmpty()) {
            throw new ValidationException(
                    "PARALLEL task requires at least one branch");
        }
        Set<String> branchNames = new HashSet<>();
        Set<String> taskNames = new HashSet<>();
        for (Branch branch : branches) {
            if (branch.getName() == null || branch.getName().isEmpty()) {
                throw new ValidationException(
                        "Each PARALLEL branch must have a non-empty 'name'");
            }
            if (!branchNames.add(branch.getName())) {
                throw new ValidationException(
                        "Duplicate branch name: " + branch.getName());
            }
            if (branch.getTasks() == null || branch.getTasks().isEmpty()) {
                throw new ValidationException(
                        "Branch '" + branch.getName() + "' must have at least one task");
            }
            for (TaskDef taskDef : branch.getTasks()) {
                if (taskDef == null || taskDef.getName() == null || taskDef.getName().isEmpty()) {
                    throw new ValidationException(
                            "Each task in PARALLEL branch '" + branch.getName()
                                    + "' must have a non-empty 'name'");
                }
                if (!taskNames.add(taskDef.getName())) {
                    throw new ValidationException(
                            "Duplicate task name across PARALLEL branches: " + taskDef.getName());
                }
            }
        }
    }

    /**
     * Creates the PARALLEL task itself AND the entry tasks for all branches.
     * <p>
     * Each branch entry task has {@code parentTaskId} set to the PARALLEL task's ID,
     * and the PARALLEL task's {@code subTaskIds} contains the IDs of all branch entry tasks.
     * <p>
     * This allows the scheduler to process all branch tasks through the normal
     * {@code decide()} loop, while maintaining the parent-child relationship.
     */
    @Override
    public List<TaskInstance> getMappedTasks(TaskMapperContext context) {

        List<Branch> branches = getParsedConfig(context.getTaskDef()).getBranches();

        // Build parallel task self.
        TaskInstance parallelTask = TaskInstance.create(context.getTaskDef());
        parallelTask.setFlowId(context.getFlowInstance().getFlowId());
        parallelTask.setInput(context.getInput());

        Map<String, Object> parallelOutput = new HashMap<>();
        parallelOutput.put(BRANCH_OUTPUTS_KEY, new HashMap<>());
        parallelTask.setOutput(parallelOutput);

        // Create one entry task for each branch, linking them to the parallel task.
        List<String> subTaskIds = new ArrayList<>(branches.size());
        List<TaskInstance> result = new ArrayList<>(branches.size() + 1);
        result.add(parallelTask);

        for (Branch branch : branches) {
            if (CollectionUtils.isNotEmpty(branch.getTasks())) {
                TaskDef branchEntryDef = branch.getTasks().get(0);

                TaskInstance branchEntry = TaskInstance.create(branchEntryDef);
                branchEntry.setFlowId(context.getFlowInstance().getFlowId());
                branchEntry.setParentTaskId(parallelTask.getTaskId());

                subTaskIds.add(branchEntry.getTaskId());
                result.add(branchEntry);
            }
        }

        parallelTask.setSubTaskIds(subTaskIds);

        return result;
    }

    /**
     * Monitors the completion of all child branch tasks.
     * <p>
     * Returns {@code false} while any branch still has pending (non-terminal) tasks,
     * causing the executor to re-evaluate this task on subsequent decide() cycles.
     * Returns {@code true} and sets status to COMPLETED when all branches are done.
     * <p>
     * For FAIL_FAST policy: if any branch task has failed, the PARALLEL task
     * immediately fails and cancels remaining branches.
     */
    @Override
    public boolean execute(TaskInstance taskInstance) {

        FlowInstance currentFlow = FlowContext.getCurrentFlow();
        if (currentFlow == null) {
            throw new IllegalStateException(
                    "FlowContext.getCurrentFlow() must not be null when executing PARALLEL task. "
                            + "Ensure FlowContext is properly set before executing.");
        }

        List<String> subTaskIds = taskInstance.getSubTaskIds();
        if (CollectionUtils.isEmpty(subTaskIds)) {
            // No branches; complete immediately
            taskInstance.setStatus(TaskStatus.COMPLETED);
            return true;
        }

        FailurePolicy failurePolicy = getFailurePolicy(taskInstance.getTaskDef());

        // Check status of all immediate child (branch entry) tasks.
        boolean allTerminal = true;
        boolean anyFailed = false;
        String failedBranchTask = null;

        for (String subTaskId : subTaskIds) {
            Optional<TaskInstance> subTaskOpt = currentFlow.getTaskById(subTaskId);
            if (!subTaskOpt.isPresent()) {
                // Sub-task not yet scheduled/persisted; not terminal.
                allTerminal = false;
                continue;
            }

            TaskInstance subTask = subTaskOpt.get();
            if (!subTask.getStatus().isTerminal()) {
                allTerminal = false;
            }

            if (subTask.getStatus().isUnsuccessfullyTerminated()) {
                anyFailed = true;
                failedBranchTask = subTask.getTaskName();
            }
        }

        // Also check all descendant tasks in each branch:
        // A branch entry task may have completed, but its successor in the
        // branch may still be running.
        if (allTerminal) {
            allTerminal = allBranchChainsTerminal(currentFlow, taskInstance);
        }

        if (failurePolicy == FailurePolicy.FAIL_FAST && anyFailed) {
            // Fail fast: cancel remaining non-terminal branches
            cancelRemainingBranches(currentFlow, taskInstance);

            Map<String, Object> output = taskInstance.getOutput();
            if (output == null) {
                output = new HashMap<>();
                taskInstance.setOutput(output);
            }
            output.put(FAILED_BRANCH_KEY, failedBranchTask);
            taskInstance.setStatus(TaskStatus.FAILED);
            taskInstance.setReasonForNotCompleting(
                    "Parallel branch failed (FAIL_FAST): " + failedBranchTask);
            return true;
        }

        if (!allTerminal) {
            // Still waiting for branches; don't mark as executed.
            return false;
        }

        // All branches complete.
        if (anyFailed) {
            // WAIT_ALL policy: all done but some failed.
            Map<String, Object> output = taskInstance.getOutput();
            if (output == null) {
                output = new HashMap<>();
                taskInstance.setOutput(output);
            }
            output.put(FAILED_BRANCH_KEY, failedBranchTask);
            taskInstance.setStatus(TaskStatus.FAILED);
            taskInstance.setReasonForNotCompleting(
                    "Parallel branch(es) failed (WAIT_ALL): " + failedBranchTask);
        } else {
            // Aggregate branch outputs.
            aggregateBranchOutputs(currentFlow, taskInstance);
            taskInstance.setStatus(TaskStatus.COMPLETED);
        }

        return true;
    }

    /**
     * Returns the failure policy from the task's input configuration.
     */
    public FailurePolicy getFailurePolicy(TaskDef taskDef) {
        if (taskDef.getInput() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> input = (Map<String, Object>) taskDef.getInput();
            Object policyValue = input.get(Options.FAILURE_POLICY.key());
            if (policyValue != null) {
                try {
                    return FailurePolicy.valueOf(String.valueOf(policyValue));
                } catch (IllegalArgumentException ignored) {
                    // fall through to default
                }
            }
        }
        return FailurePolicy.FAIL_FAST;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TaskDef next(final TaskDef toBeSearched, final TaskDef target) {

        if (!getType().equals(toBeSearched.getType())) {
            throw new IllegalArgumentException(
                    String.format("The 'next' method cannot be executed, " +
                                    "because the to be searched task type does not match, %s != %s",
                            getType(), toBeSearched.getType()));
        }

        if (target == null) {
            throw new IllegalArgumentException("The target task is null");
        }

        final FlowInstance currentFlow = FlowContext.getCurrentFlow();

        Optional<TaskInstance> mappingTaskOptional =
                currentFlow.getTaskByName(toBeSearched.getName());

        if (!mappingTaskOptional.isPresent()) {
            return null;
        }

        TaskInstance mappingTask = mappingTaskOptional.get();

        if (mappingTask.getOutput() == null) {
            throw new IllegalStateException(
                    "PARALLEL task output must not be null when determining next task");
        }

        final ParsedConfig parsedConfig = getParsedConfig(toBeSearched);
        final List<Branch> branches = parsedConfig.getBranches();

        if (CollectionUtils.isEmpty(branches)) {
            return null;
        }

        // When the target is the PARALLEL task itself (self-reference from decide()),
        // return MATCHED so the iterator pattern in FlowExecutor.getNextTask() correctly
        // advances to the next sibling task. Branch entry tasks are already scheduled
        // by getMappedTasks(), so we don't return a branch task here.
        if (toBeSearched == target
                || toBeSearched.getName().equals(target.getName())) {
            return TaskDef.MATCHED;
        }

        // Fast path: if target is a configured top-level branch task, locate branch directly.
        Branch indexedBranch = parsedConfig.getTopLevelTaskToBranch().get(target.getName());
        if (indexedBranch != null) {
            return findNextTaskFromChildren(indexedBranch.getTasks(), target);
        }

        // Fallback path: traverse all branches for nested control-flow tasks.
        for (Branch branch : branches) {
            if (CollectionUtils.isNotEmpty(branch.getTasks())) {
                TaskDef nextTask = findNextTaskFromChildren(branch.getTasks(), target);
                if (nextTask != null) {
                    return nextTask;
                }
            }
        }

        return null;
    }

    /**
     * Returns the first TaskDef for each branch.
     */
    public List<TaskDef> getBranchEntryTasks(TaskDef taskDef) {
        List<Branch> branches = getParsedConfig(taskDef).getBranches();
        List<TaskDef> entryTasks = new ArrayList<>(branches.size());
        for (Branch branch : branches) {
            if (CollectionUtils.isNotEmpty(branch.getTasks())) {
                entryTasks.add(branch.getTasks().get(0));
            }
        }
        return entryTasks;
    }

    @Override
    public void cancel(TaskInstance taskInstance) {
        if (taskInstance.getStatus().isTerminal()) {
            return;
        }
        taskInstance.setStatus(TaskStatus.CANCELED);

        // Also cancel child branch tasks via FlowExecutor.
        FlowInstance currentFlow = FlowContext.getCurrentFlow();
        if (currentFlow != null
                && CollectionUtils.isNotEmpty(taskInstance.getSubTaskIds())) {
            cancelRemainingBranches(currentFlow, taskInstance);
        }
    }

    /**
     * Returns the list of branches parsed from the task definition.
     */
    @SuppressWarnings("unchecked")
    List<Branch> getBranches(@NotNull final TaskDef taskDef) {
        return getParsedConfig(taskDef).getBranches();
    }

    @SuppressWarnings("unchecked")
    private ParsedConfig getParsedConfig(@NotNull final TaskDef taskDef) {
        ParsedConfig parsedConfig = taskDef.getParsed();

        if (parsedConfig == null) {
            synchronized (taskDef) {
                parsedConfig = taskDef.getParsed();
                if (parsedConfig == null) {

                    if (!(taskDef.getInput() instanceof Map)) {
                        throw new IllegalArgumentException(
                                "PARALLEL task input must be a Map");
                    }

                    List<Branch> branches = JsonUtils.convertValue(
                            ((Map<String, Object>) taskDef.getInput())
                                    .get(Options.BRANCHES.key()),
                            new TypeReference<List<Branch>>() {
                            }
                    );

                    Map<String, Branch> topLevelTaskToBranch = new LinkedHashMap<>();
                    Set<String> allBranchTaskNames = new HashSet<>();
                    for (Branch branch : branches) {
                        if (CollectionUtils.isNotEmpty(branch.getTasks())) {
                            for (TaskDef branchTask : branch.getTasks()) {
                                allBranchTaskNames.add(branchTask.getName());
                                topLevelTaskToBranch.putIfAbsent(branchTask.getName(), branch);
                            }
                        }
                    }

                    parsedConfig = new ParsedConfig(
                            branches,
                            Collections.unmodifiableSet(allBranchTaskNames),
                            Collections.unmodifiableMap(topLevelTaskToBranch)
                    );
                    taskDef.setParsed(parsedConfig);
                }
            }
        }
        return parsedConfig;
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /**
     * Checks whether all branch chains (from entry to last task) have reached
     * a terminal status. We collect the set of task names belonging to all
     * branches and check that every matching TaskInstance is terminal.
     */
    private boolean allBranchChainsTerminal(
            final FlowInstance currentFlow,
            final TaskInstance parallelTask) {

        Set<String> branchTaskNames =
                getParsedConfig(parallelTask.getTaskDef()).getAllBranchTaskNames();

        return currentFlow.getTaskInstances().stream()
                .filter(t -> branchTaskNames.contains(t.getTaskName()))
                .allMatch(t -> t.getStatus().isTerminal());
    }

    /**
     * Cancels all non-terminal branch tasks by delegating to
     * {@link FlowExecutor#cancelTask(TaskInstance)}. This ensures that
     * nested control-flow tasks (e.g., sub-flows, nested PARALLEL or LOOP
     * tasks) are properly cleaned up rather than just having their status
     * changed directly.
     */
    private void cancelRemainingBranches(
            final FlowInstance currentFlow,
            final TaskInstance parallelTask) {

        Set<String> branchTaskNames =
                getParsedConfig(parallelTask.getTaskDef()).getAllBranchTaskNames();

        for (TaskInstance task : currentFlow.getTaskInstances()) {
            if (!task.getStatus().isTerminal()
                    && branchTaskNames.contains(task.getTaskName())) {
                if (flowExecutor != null) {
                    flowExecutor.cancelTask(task);
                } else {
                    task.setStatus(TaskStatus.CANCELED);
                }
            }
        }
    }

    /**
     * Aggregates branch outputs into the PARALLEL task's output.
     * For each branch, finds the last terminal task and records its output.
     */
    @SuppressWarnings("unchecked")
    private void aggregateBranchOutputs(
            final FlowInstance currentFlow,
            final TaskInstance parallelTask) {

        Map<String, Object> output = parallelTask.getOutput();
        if (output == null) {
            output = new HashMap<>();
            parallelTask.setOutput(output);
        }

        Map<String, Object> branchOutputs =
                (Map<String, Object>) output.computeIfAbsent(
                        BRANCH_OUTPUTS_KEY, k -> new HashMap<>());

        List<Branch> branches = getBranches(parallelTask.getTaskDef());

        for (Branch branch : branches) {
            if (branch.getTasks() == null || branch.getTasks().isEmpty()) {
                continue;
            }

            // The last task defined in the branch is the output source.
            String lastTaskName = branch.getTasks()
                    .get(branch.getTasks().size() - 1).getName();

            currentFlow.getTaskByName(lastTaskName).ifPresent(lastTask -> {
                if (lastTask.getStatus().isTerminal()) {
                    branchOutputs.put(branch.getName(), lastTask.getOutput());
                }
            });
        }
    }

    /**
     * Traverses the child tasks using {@code flowExecutor.getNextTask()},
     * following the same pattern as IFTask/SwitchTask. This properly supports
     * nested control-flow tasks (IF/SWITCH/LOOP/SUB_FLOW) within branches.
     */
    @SuppressWarnings("unchecked")
    private TaskDef findNextTaskFromChildren(
            final List<TaskDef> children,
            final TaskDef target) {

        if (flowExecutor == null) {
            throw new IllegalStateException("flowExecutor is not initialized for PARALLEL task");
        }

        Iterator<TaskDef> iterator = children.iterator();

        while (iterator.hasNext()) {
            TaskDef nextTask =
                    flowExecutor.getNextTask(iterator.next(), target);
            if (nextTask == TaskDef.MATCHED) {
                return iterator.hasNext()
                        ? iterator.next()
                        : TaskDef.MATCHED;
            } else if (nextTask != null) {
                return nextTask;
            }
        }

        return null;
    }

    /**
     * Failure policy for parallel branches.
     */
    public enum FailurePolicy {
        /**
         * Fail the entire PARALLEL task as soon as any branch fails.
         */
        FAIL_FAST,

        /**
         * Wait for all branches to complete, then report aggregated result.
         */
        WAIT_ALL
    }

    static class Options {

        static final ConfigOption<List<Branch>> BRANCHES = ConfigOptions
                .key("branches")
                .classType(Branch.class)
                .asList()
                .noDefaultValue()
                .withDescription("The parallel branches to execute concurrently.");

        static final ConfigOption<String> FAILURE_POLICY = ConfigOptions
                .key("failurePolicy")
                .stringType()
                .defaultValue("FAIL_FAST")
                .withDescription("Failure handling policy: FAIL_FAST or WAIT_ALL.");
    }

    @Data
    public static class Branch implements Serializable {

        private static final long serialVersionUID = 7823456789012345678L;

        private String name;

        private List<TaskDef> tasks;
    }

    @Data
    private static class ParsedConfig implements Serializable {

        private static final long serialVersionUID = -1019391089011710638L;

        private final List<Branch> branches;

        private final Set<String> allBranchTaskNames;

        private final Map<String, Branch> topLevelTaskToBranch;
    }
}
