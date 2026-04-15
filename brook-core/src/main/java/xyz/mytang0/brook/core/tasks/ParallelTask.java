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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Parallel task, used to execute multiple branches concurrently.
 * <p>
 * Each branch contains one or more sequential TaskDefs that are
 * executed in parallel with other branches. The PARALLEL task
 * completes when all branches complete (or fails according to the
 * configured failure policy).
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

        return Collections.singletonList(parallelTask);
    }

    @Override
    public boolean execute(TaskInstance taskInstance) {
        taskInstance.setStatus(TaskStatus.COMPLETED);
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

        if (toBeSearched == target
                || toBeSearched.getName().equals(target.getName())) {
            // Starting the parallel task: return the first task of the first branch.
            // The executor will detect PARALLEL type and schedule all branches' first tasks.
            // Here we just need to return the first child of branch 0 so the executor
            // can find the entry point.
            TaskDef firstBranchFirstTask = createBranchTaskDef(
                    branches.get(0).getTasks().get(0), 0);
            output.put(INNER_LAST_TASK, firstBranchFirstTask.getName());
            return firstBranchFirstTask;
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
     * This is called by the executor to get all parallel branch entry points.
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
