package xyz.mytang0.brook.core.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.constants.TaskConstants;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
 * Child task names are suffixed with {@code __PARALLEL_<branchIndex>}
 * per branch to ensure uniqueness across branches (FlowExecutor
 * deduplicates tasks by name).
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
public class ParallelTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("PARALLEL")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Parallel task, executes multiple branches concurrently.");

    // Output keys.
    static final String BRANCH_OUTPUTS_KEY = "branchOutputs";

    static final String FAILED_BRANCH_KEY = "failedBranch";

    static final String INNER_LAST_TASK = "innerLastTask";

    // Keys that are specific to TaskDef (beyond "name" and "type") used to
    // distinguish real TaskDef Maps from arbitrary user payload Maps.
    private static final Set<String> TASK_DEF_INDICATOR_KEYS =
            new HashSet<>(Arrays.asList(
                    "input", "controlDef", "progressDef", "logDef", "linkDef",
                    "checkDef", "hangDef", "callback", "extension", "template"
            ));

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

        List<Branch> branches = getBranches(context.getTaskDef());

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

        for (int i = 0; i < branches.size(); i++) {
            Branch branch = branches.get(i);
            if (CollectionUtils.isNotEmpty(branch.getTasks())) {
                TaskDef branchEntryDef = createBranchTaskDef(
                        branch.getTasks().get(0), i);

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
        // A branch is "done" when its entry task has been executed (is terminal)
        // AND has no further pending tasks in the branch chain.
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
        // branch may still be running. We check that the last task in each
        // branch chain has reached a terminal state.
        if (allTerminal) {
            allTerminal = allBranchChainsTerminal(
                    currentFlow, taskInstance, subTaskIds);
        }

        if (failurePolicy == FailurePolicy.FAIL_FAST && anyFailed) {
            // Fail fast: cancel remaining non-terminal branches
            cancelRemainingBranches(currentFlow, subTaskIds);

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
            aggregateBranchOutputs(currentFlow, taskInstance, subTaskIds);
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

    @SuppressWarnings("all")
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

        Map<String, Object> output = mappingTask.getOutput();

        final List<Branch> branches = getBranches(toBeSearched);

        if (CollectionUtils.isEmpty(branches)) {
            return null;
        }

        // When the target is the PARALLEL task itself (self-reference from decide()),
        // we should NOT return any branch entry task — branches are already scheduled
        // by getMappedTasks(). Return null so the flow proceeds past PARALLEL when done.
        if (toBeSearched == target
                || toBeSearched.getName().equals(target.getName())) {
            return null;
        }

        // Find which branch the target belongs to.
        int branchIndex = extractBranchIndex(target.getName());
        if (branchIndex < 0) {
            return null;
        }

        if (branchIndex >= branches.size()) {
            return null;
        }

        Branch branch = branches.get(branchIndex);
        TaskDef nextTask = findNextTaskFromChildren(
                branch.getTasks(), target, branchIndex);

        if (nextTask != null && nextTask != TaskDef.MATCHED) {
            output.put(INNER_LAST_TASK, nextTask.getName());
        }

        return nextTask;
    }

    /**
     * Returns the first TaskDef for each branch, with branch-specific name suffixes.
     */
    public List<TaskDef> getBranchEntryTasks(TaskDef taskDef) {
        List<Branch> branches = getBranches(taskDef);
        List<TaskDef> entryTasks = new ArrayList<>(branches.size());
        for (int i = 0; i < branches.size(); i++) {
            Branch branch = branches.get(i);
            if (CollectionUtils.isNotEmpty(branch.getTasks())) {
                entryTasks.add(createBranchTaskDef(
                        branch.getTasks().get(0), i));
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

        // Also cancel child branch tasks if flow context is available.
        FlowInstance currentFlow = FlowContext.getCurrentFlow();
        if (currentFlow != null
                && CollectionUtils.isNotEmpty(taskInstance.getSubTaskIds())) {
            cancelRemainingBranches(currentFlow, taskInstance.getSubTaskIds());
        }
    }

    /**
     * Returns the list of branches parsed from the task definition.
     */
    @SuppressWarnings("all")
    List<Branch> getBranches(@NotNull final TaskDef taskDef) {
        List<Branch> branches = taskDef.getParsed();

        if (branches == null) {
            synchronized (taskDef) {

                branches = taskDef.getParsed();

                if (branches == null) {

                    if (!(taskDef.getInput() instanceof Map)) {
                        throw new IllegalArgumentException(
                                "PARALLEL task input must be a Map");
                    }

                    branches = JsonUtils.convertValue(
                            ((Map<String, Object>) taskDef.getInput())
                                    .get(Options.BRANCHES.key()),
                            new TypeReference<List<Branch>>() {
                            }
                    );

                    taskDef.setParsed(branches);
                }
            }
        }
        return branches;
    }

    // ========================================================================
    // Internal helpers
    // ========================================================================

    /**
     * Checks whether all branch chains (from entry to last task) have reached
     * a terminal status. A "branch chain" is the sequence of tasks that follow
     * from a branch entry task within the PARALLEL task's scope.
     */
    private boolean allBranchChainsTerminal(
            final FlowInstance currentFlow,
            final TaskInstance parallelTask,
            final List<String> subTaskIds) {

        for (String subTaskId : subTaskIds) {
            Optional<TaskInstance> subTaskOpt = currentFlow.getTaskById(subTaskId);
            if (!subTaskOpt.isPresent()) {
                return false;
            }

            TaskInstance subTask = subTaskOpt.get();
            if (!subTask.getStatus().isTerminal()) {
                return false;
            }

            // Check if there are further tasks in this branch that
            // haven't completed yet. We look for tasks whose name contains
            // the same branch suffix.
            int branchIndex = extractBranchIndex(subTask.getTaskName());
            if (branchIndex >= 0) {
                String branchSuffix = TaskConstants.PARALLEL_INDEX_SEPARATOR + branchIndex;
                boolean hasPendingBranchTask = currentFlow.getTaskInstances().stream()
                        .filter(t -> t.getTaskName().endsWith(branchSuffix))
                        .anyMatch(t -> !t.getStatus().isTerminal());
                if (hasPendingBranchTask) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Cancels all non-terminal branch tasks.
     */
    private void cancelRemainingBranches(
            final FlowInstance currentFlow,
            final List<String> subTaskIds) {

        // Collect all branch suffixes from sub-task entries.
        Set<String> branchSuffixes = new HashSet<>();
        for (String subTaskId : subTaskIds) {
            currentFlow.getTaskById(subTaskId).ifPresent(task -> {
                int idx = extractBranchIndex(task.getTaskName());
                if (idx >= 0) {
                    branchSuffixes.add(
                            TaskConstants.PARALLEL_INDEX_SEPARATOR + idx);
                }
            });
        }

        // Cancel all non-terminal tasks belonging to any of these branches.
        for (TaskInstance task : currentFlow.getTaskInstances()) {
            if (!task.getStatus().isTerminal()) {
                for (String suffix : branchSuffixes) {
                    if (task.getTaskName().endsWith(suffix)) {
                        task.setStatus(TaskStatus.CANCELED);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Aggregates branch outputs into the PARALLEL task's output.
     */
    @SuppressWarnings("unchecked")
    private void aggregateBranchOutputs(
            final FlowInstance currentFlow,
            final TaskInstance parallelTask,
            final List<String> subTaskIds) {

        Map<String, Object> output = parallelTask.getOutput();
        if (output == null) {
            output = new HashMap<>();
            parallelTask.setOutput(output);
        }

        Map<String, Object> branchOutputs =
                (Map<String, Object>) output.computeIfAbsent(
                        BRANCH_OUTPUTS_KEY, k -> new HashMap<>());

        List<Branch> branches = getBranches(parallelTask.getTaskDef());

        for (int i = 0; i < branches.size() && i < subTaskIds.size(); i++) {
            Branch branch = branches.get(i);
            String branchSuffix = TaskConstants.PARALLEL_INDEX_SEPARATOR + i;

            // Find the last terminal task in this branch.
            TaskInstance lastTask = null;
            for (TaskInstance task : currentFlow.getTaskInstances()) {
                if (task.getTaskName().endsWith(branchSuffix)
                        && task.getStatus().isTerminal()) {
                    lastTask = task;
                }
            }

            if (lastTask != null) {
                branchOutputs.put(branch.getName(), lastTask.getOutput());
            }
        }
    }

    /**
     * Creates a deep copy of a TaskDef with a branch-specific name suffix.
     * This ensures each branch's tasks have unique names, preventing
     * the executor's name-based deduplication from filtering them out.
     */
    private TaskDef createBranchTaskDef(TaskDef original, int branchIndex) {
        String suffix = TaskConstants.PARALLEL_INDEX_SEPARATOR + branchIndex;

        TaskDef copy = new TaskDef();
        copy.setType(original.getType());
        copy.setName(original.getName() + suffix);
        copy.setDisplay(original.getDisplay());
        copy.setDescription(original.getDescription());
        copy.setControlDef(original.getControlDef());
        copy.setProgressDef(original.getProgressDef());
        copy.setLogDef(original.getLogDef());
        copy.setLinkDef(original.getLinkDef());
        copy.setCheckDef(original.getCheckDef());
        copy.setHangDef(original.getHangDef());
        copy.setCallback(original.getCallback());
        copy.setExtension(original.getExtension());
        copy.setTemplate(original.getTemplate());

        Object copiedInput = deepCopyInputObject(original.getInput());
        copy.setInput(copiedInput);
        copy.setOutput(original.getOutput());

        // Recursively rename any nested TaskDefs embedded in the input.
        if (copiedInput != null) {
            renameNestedTaskDefs(copiedInput, suffix);
        }
        return copy;
    }

    @SuppressWarnings("unchecked")
    private Object deepCopyInputObject(Object input) {
        if (input instanceof Map) {
            Map<String, Object> original = (Map<String, Object>) input;
            Map<String, Object> copied = new HashMap<>(original.size());
            for (Map.Entry<String, Object> entry : original.entrySet()) {
                copied.put(entry.getKey(), deepCopyInputObject(entry.getValue()));
            }
            return copied;
        }
        if (input instanceof List) {
            List<?> original = (List<?>) input;
            List<Object> copied = new ArrayList<>(original.size());
            for (Object item : original) {
                copied.add(deepCopyInputObject(item));
            }
            return copied;
        }
        return input;
    }

    @SuppressWarnings("unchecked")
    private void renameNestedTaskDefs(Object obj, String suffix) {
        if (obj instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) obj;
            if (isTaskDefMap(map)) {
                map.put("name", map.get("name") + suffix);
            }
            for (Object value : map.values()) {
                renameNestedTaskDefs(value, suffix);
            }
        } else if (obj instanceof List) {
            for (Object item : (List<?>) obj) {
                renameNestedTaskDefs(item, suffix);
            }
        }
    }

    private static boolean isTaskDefMap(Map<String, Object> map) {
        Object name = map.get("name");
        if (!(name instanceof String)) {
            return false;
        }

        Object type = map.get("type");
        if (!(type instanceof String)) {
            return false;
        }

        for (String key : TASK_DEF_INDICATOR_KEYS) {
            if (map.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extracts the branch index from a suffixed task name.
     * Returns -1 if the name has no branch suffix.
     */
    public static int extractBranchIndex(String taskName) {
        int sepIndex = taskName.lastIndexOf(TaskConstants.PARALLEL_INDEX_SEPARATOR);
        if (sepIndex >= 0) {
            try {
                return Integer.parseInt(
                        taskName.substring(
                                sepIndex
                                        + TaskConstants.PARALLEL_INDEX_SEPARATOR.length()));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    @SuppressWarnings("all")
    private TaskDef findNextTaskFromChildren(
            final List<TaskDef> branchTasks,
            final TaskDef target,
            final int branchIndex) {

        for (int i = 0; i < branchTasks.size(); i++) {
            TaskDef nextTask = flowExecutor.getNextTask(
                    createBranchTaskDef(branchTasks.get(i), branchIndex),
                    target
            );
            if (nextTask == TaskDef.MATCHED) {
                return (i + 1) < branchTasks.size()
                        ? createBranchTaskDef(branchTasks.get(i + 1), branchIndex)
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
}
