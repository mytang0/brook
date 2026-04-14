package xyz.mytang0.brook.core.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.context.FlowContext;
import xyz.mytang0.brook.common.context.TaskMapperContext;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.core.FlowExecutor;
import xyz.mytang0.brook.spi.computing.EngineActuator;
import xyz.mytang0.brook.spi.task.FlowTask;

import javax.validation.ValidationException;
import javax.validation.constraints.NotNull;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static xyz.mytang0.brook.core.utils.ParameterUtils.flowContext;

/**
 * Loop task, used to iterate over a collection and execute child tasks for each element.
 * <p>
 * Supports two mutually exclusive modes:
 * <ul>
 *   <li>{@code loopOver}: iterate over a list/collection expression</li>
 *   <li>{@code loopCount}: iterate a fixed number of times</li>
 * </ul>
 * <p>
 * During each iteration, the loop output exposes {@code currentIndex} and {@code currentItem}
 * (when using loopOver) so that child tasks can reference them via
 * {@code ${loopTaskName.output.currentIndex}} and {@code ${loopTaskName.output.currentItem}}.
 * <p>
 * Child task names are suffixed with {@code __LOOP_<index>} per iteration to ensure
 * uniqueness across iterations (FlowExecutor deduplicates tasks by name).
 */
public class LoopTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("LOOP")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Loop task, used to iterate and execute child tasks for each element.");

    // Output keys.
    static final String CURRENT_INDEX_KEY = "currentIndex";

    static final String CURRENT_ITEM_KEY = "currentItem";

    static final String ITERATIONS_KEY = "iterations";

    static final String INNER_LAST_TASK = "innerLastTask";

    // Separator used to create per-iteration unique task names.
    static final String LOOP_INDEX_SEPARATOR = "__LOOP_";

    private final EngineActuator engineActuator;

    @Setter
    private FlowExecutor<?> flowExecutor;

    public LoopTask() {
        this.engineActuator = ExtensionDirector
                .getExtensionLoader(EngineActuator.class)
                .getDefaultExtension();
    }

    @Override
    public ConfigOption<?> catalog() {
        return CATALOG;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(Options.LOOP_BODY);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(Options.ENGINE_TYPE);
        options.add(Options.LOOP_OVER);
        options.add(Options.LOOP_COUNT);
        return options;
    }

    @Override
    public void doVerify(@NotNull Configuration configuration) {
        boolean hasLoopOver = configuration.contains(Options.LOOP_OVER);
        boolean hasLoopCount = configuration.contains(Options.LOOP_COUNT);
        if (!hasLoopOver && !hasLoopCount) {
            throw new ValidationException(
                    "Exactly one of 'loopOver' or 'loopCount' must be specified");
        }
        if (hasLoopOver && hasLoopCount) {
            throw new ValidationException(
                    "'loopOver' and 'loopCount' are mutually exclusive, specify only one");
        }
    }

    @SuppressWarnings("all")
    @Override
    public List<TaskInstance> getMappedTasks(TaskMapperContext context) {

        Configuration taskDefInput = context.getInputConfiguration();

        int iterations;
        List<Object> loopOverValue = null;

        // Determine iteration count.
        if (taskDefInput.contains(Options.LOOP_OVER)) {
            Object loopOverRaw = taskDefInput.get(Options.LOOP_OVER);

            // Only invoke the compute engine when the value is still a string expression.
            // If the parameter has already been resolved to a List by parameter mapping,
            // passing it through the engine would break (e.g., "[a, b]" is not a valid expression).
            Object evaluated;
            if (loopOverRaw instanceof String) {
                evaluated = Optional.ofNullable(taskDefInput.getString(Options.ENGINE_TYPE))
                        .filter(StringUtils::isNotBlank)
                        .map(engineType ->
                                engineActuator.compute(
                                        engineType,
                                        (String) loopOverRaw,
                                        flowContext(context.getFlowInstance())
                                )
                        )
                        .orElse(loopOverRaw);
            } else {
                evaluated = loopOverRaw;
            }

            // Normalize to List: accept List, Collection, Iterable, and arrays.
            if (evaluated == null) {
                throw new IllegalArgumentException(
                        "The loop task 'loopOver' must evaluate to a list/collection/iterable, got: null");
            } else if (evaluated instanceof List) {
                loopOverValue = (List<Object>) evaluated;
            } else if (evaluated instanceof Collection) {
                loopOverValue = new ArrayList<>((Collection<?>) evaluated);
            } else if (evaluated instanceof Iterable) {
                loopOverValue = new ArrayList<>();
                for (Object item : (Iterable<?>) evaluated) {
                    loopOverValue.add(item);
                }
            } else if (evaluated.getClass().isArray()) {
                int length = Array.getLength(evaluated);
                loopOverValue = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    loopOverValue.add(Array.get(evaluated, i));
                }
            } else {
                throw new IllegalArgumentException(
                        "The loop task 'loopOver' must evaluate to a list/collection/iterable, got: "
                                + evaluated.getClass().getName());
            }
            iterations = loopOverValue.size();
        } else {
            Integer loopCount = taskDefInput.get(Options.LOOP_COUNT);
            if (loopCount == null || loopCount < 0) {
                throw new IllegalArgumentException(
                        "The loop task 'loopCount' must be specified and be a non-negative integer");
            }
            iterations = loopCount;
        }

        // Build loop task self.
        TaskInstance loopTask = TaskInstance.create(context.getTaskDef());
        loopTask.setFlowId(context.getFlowInstance().getFlowId());
        loopTask.setInput(context.getInput());

        // Persist the normalized list into the task input so that
        // updateCurrentItem() can reliably index into it on later iterations,
        // even when the original loopOver was an engine expression or a
        // non-List collection/array that was normalized above.
        if (loopOverValue != null && loopTask.getInput() instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> inputMap = (Map<String, Object>) loopTask.getInput();
            inputMap.put(Options.LOOP_OVER.key(), loopOverValue);
        }

        Map<String, Object> loopOutput = new HashMap<>();
        loopOutput.put(ITERATIONS_KEY, iterations);
        loopOutput.put(CURRENT_INDEX_KEY, 0);
        if (loopOverValue != null && !loopOverValue.isEmpty()) {
            loopOutput.put(CURRENT_ITEM_KEY, loopOverValue.get(0));
        }
        loopTask.setOutput(loopOutput);

        return Collections.singletonList(loopTask);
    }

    @Override
    public boolean execute(TaskInstance taskInstance) {
        taskInstance.setStatus(TaskStatus.COMPLETED);
        return true;
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
                    "LOOP task output must not be null when determining next task");
        }

        Map<String, Object> output = mappingTask.getOutput();

        int iterations = (int) output.get(ITERATIONS_KEY);

        // No iterations needed.
        if (iterations <= 0) {
            return null;
        }

        final List<TaskDef> loopBody = getLoopBody(toBeSearched);

        if (CollectionUtils.isEmpty(loopBody)) {
            return null;
        }

        TaskDef nextTask;

        if (toBeSearched == target
                || toBeSearched.getName().equals(target.getName())) {
            // Starting the loop: return first child task of iteration 0.
            output.put(CURRENT_INDEX_KEY, 0);
            nextTask = createIterationTaskDef(loopBody.get(0), 0);
        } else {
            // Find the target's position in the loop body by stripping the iteration suffix.
            String originalName = stripIterationSuffix(target.getName());
            int iterationIndex = extractIterationIndex(target.getName());

            int pos = findPositionInLoopBody(loopBody, originalName);

            if (pos < 0) {
                // Target not found in loop body.
                return null;
            }

            int effectiveIndex = iterationIndex >= 0
                    ? iterationIndex
                    : (int) output.get(CURRENT_INDEX_KEY);

            if (pos + 1 < loopBody.size()) {
                // More tasks in the current iteration.
                nextTask = createIterationTaskDef(
                        loopBody.get(pos + 1), effectiveIndex);
            } else {
                // Last task in the current iteration, advance to next.
                int nextIndex = effectiveIndex + 1;
                if (nextIndex < iterations) {
                    output.put(CURRENT_INDEX_KEY, nextIndex);
                    updateCurrentItem(output, mappingTask, nextIndex);
                    nextTask = createIterationTaskDef(
                            loopBody.get(0), nextIndex);
                } else {
                    // All iterations done.
                    nextTask = TaskDef.MATCHED;
                }
            }
        renameIterationTaskDefs(copy, iterationIndex);
        return copy;
    }

    private void renameIterationTaskDefs(TaskDef root, int iterationIndex) {
        Set<Object> visited = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        renameIterationTaskDefsRecursively(root, iterationIndex, visited);
    }

    private void renameIterationTaskDefsRecursively(
            Object node, int iterationIndex, Set<Object> visited) {
        if (node == null || visited.contains(node)) {
            return;
        }
        visited.add(node);

        if (node instanceof TaskDef) {
            TaskDef taskDef = (TaskDef) node;
            if (taskDef.getName() != null) {
                taskDef.setName(taskDef.getName() + LOOP_INDEX_SEPARATOR + iterationIndex);
            }
        }

        if (node instanceof Map) {
            for (Object value : ((Map<?, ?>) node).values()) {
                renameIterationTaskDefsRecursively(value, iterationIndex, visited);
            }
            return;
        }

        if (node instanceof Iterable) {
            for (Object value : (Iterable<?>) node) {
                renameIterationTaskDefsRecursively(value, iterationIndex, visited);
            }
            return;
        }

        Class<?> nodeClass = node.getClass();
        if (nodeClass.isArray()) {
            int length = java.lang.reflect.Array.getLength(node);
            for (int i = 0; i < length; i++) {
                renameIterationTaskDefsRecursively(
                        java.lang.reflect.Array.get(node, i), iterationIndex, visited);
            }
            return;
        }

        if (isTerminalObject(nodeClass)) {
            return;
        }

        Class<?> currentClass = nodeClass;
        while (currentClass != null && currentClass != Object.class) {
            for (java.lang.reflect.Field field : currentClass.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())
                        || field.getType().isPrimitive()
                        || field.isSynthetic()) {
                    continue;
                }
                try {
                    field.setAccessible(true);
                    renameIterationTaskDefsRecursively(
                            field.get(node), iterationIndex, visited);
                } catch (IllegalAccessException ignored) {
                    // Best-effort traversal: inaccessible fields are skipped.
                }
            }
            currentClass = currentClass.getSuperclass();
        }
    }

    private boolean isTerminalObject(Class<?> type) {
        return type.isEnum()
                || String.class.equals(type)
                || Number.class.isAssignableFrom(type)
                || Boolean.class.equals(type)
                || Character.class.equals(type)
                || Class.class.equals(type)
                || type.getName().startsWith("java.time.")
                || type.getName().startsWith("java.lang.");
    }
        }

        return nextTask;
    }

    /**
     * Creates a deep copy of a TaskDef with an iteration-specific name suffix.
     * This ensures each iteration's tasks have unique names, preventing
     * the executor's name-based deduplication from filtering them out.
     * <p>
     * Recursively renames all nested child TaskDefs (e.g., inside IF/SWITCH
     * branches) to prevent name collisions across iterations.
     */
    private TaskDef createIterationTaskDef(TaskDef original, int iterationIndex) {
        String suffix = LOOP_INDEX_SEPARATOR + iterationIndex;
        TaskDef copy = JsonUtils.readValue(
                JsonUtils.toJsonString(original), TaskDef.class);
        copy.setName(original.getName() + suffix);
        // Recursively rename any nested TaskDefs embedded in the input
        // (e.g., IF trueBranch/falseBranch, SWITCH cases, nested LOOP body).
        if (copy.getInput() != null) {
            renameNestedTaskDefs(copy.getInput(), suffix);
        }
        return copy;
    }

    /**
     * Recursively walks an object tree (Maps and Lists from JSON) and renames
     * any Map that looks like a TaskDef (has both "name" and "type" keys) by
     * appending the given suffix to its "name" value.
     */
    @SuppressWarnings("unchecked")
    private void renameNestedTaskDefs(Object obj, String suffix) {
        if (obj instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) obj;
            if (map.containsKey("name") && map.containsKey("type")
                    && map.get("name") instanceof String) {
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

    /**
     * Strips the iteration suffix ({@code __LOOP_N}) from a task name
     * to recover the original name defined in loopBody.
     */
    static String stripIterationSuffix(String taskName) {
        int sepIndex = taskName.lastIndexOf(LOOP_INDEX_SEPARATOR);
        return sepIndex >= 0
                ? taskName.substring(0, sepIndex)
                : taskName;
    }

    /**
     * Extracts the iteration index from a suffixed task name.
     * Returns -1 if the name has no iteration suffix.
     */
    static int extractIterationIndex(String taskName) {
        int sepIndex = taskName.lastIndexOf(LOOP_INDEX_SEPARATOR);
        if (sepIndex >= 0) {
            try {
                return Integer.parseInt(
                        taskName.substring(
                                sepIndex + LOOP_INDEX_SEPARATOR.length()));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Finds the position of a task in the loop body by its original name.
     */
    private int findPositionInLoopBody(List<TaskDef> loopBody,
                                       String originalName) {
        for (int i = 0; i < loopBody.size(); i++) {
            if (loopBody.get(i).getName().equals(originalName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Updates the currentItem in the output for the given iteration index.
     * Retrieves the loopOver collection from the LOOP task's resolved input,
     * avoiding the need to store the full collection in the output.
     */
    @SuppressWarnings("all")
    private void updateCurrentItem(Map<String, Object> output,
                                   TaskInstance mappingTask,
                                   int nextIndex) {
        Map<String, Object> input = mappingTask.getInput();
        if (input != null) {
            Object loopOver = input.get(Options.LOOP_OVER.key());
            if (loopOver instanceof List) {
                List<Object> loopOverValue = (List<Object>) loopOver;
                if (nextIndex < loopOverValue.size()) {
                    output.put(CURRENT_ITEM_KEY, loopOverValue.get(nextIndex));
                }
            }
        }
    }

    @SuppressWarnings("all")
    private List<TaskDef> getLoopBody(@NotNull final TaskDef taskDef) {
        List<TaskDef> loopBody = taskDef.getParsed();

        if (loopBody == null) {
            synchronized (taskDef) {

                loopBody = taskDef.getParsed();

                if (loopBody == null) {

                    if (!(taskDef.getInput() instanceof Map)) {
                        throw new IllegalArgumentException(
                                "Loop task input must be a Map");
                    }

                    loopBody = JsonUtils.convertValue(
                            ((Map<String, Object>) taskDef.getInput())
                                    .get(Options.LOOP_BODY.key()),
                            new TypeReference<List<TaskDef>>() {
                            }
                    );

                    taskDef.setParsed(loopBody);
                }
            }
        }
        return loopBody;
    }

    static class Options {

        static final ConfigOption<String> ENGINE_TYPE = ConfigOptions
                .key("engineType")
                .stringType()
                .noDefaultValue()
                .withDescription("The expression evaluation engine type for loopOver.");

        static final ConfigOption<Object> LOOP_OVER = ConfigOptions
                .key("loopOver")
                .classType(Object.class)
                .noDefaultValue()
                .withDescription("The collection to iterate over. " +
                        "Can be a list literal or an expression. " +
                        "Mutually exclusive with 'loopCount'.");

        static final ConfigOption<Integer> LOOP_COUNT = ConfigOptions
                .key("loopCount")
                .intType()
                .noDefaultValue()
                .withDescription("The number of iterations to perform. " +
                        "Mutually exclusive with 'loopOver'.");

        static final ConfigOption<List<TaskDef>> LOOP_BODY = ConfigOptions
                .key("loopBody")
                .classType(TaskDef.class)
                .asList()
                .noDefaultValue()
                .withDescription("The list of child task definitions to execute for each iteration.");
    }
}
