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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static xyz.mytang0.brook.core.utils.ParameterUtils.flowContext;

/**
 * Loop task, used to iterate over a collection and execute child tasks for each element.
 * <p>
 * Supports two modes:
 * <ul>
 *   <li>{@code loopOver}: iterate over a list/collection expression</li>
 *   <li>{@code loopCount}: iterate a fixed number of times</li>
 * </ul>
 * <p>
 * During each iteration, the loop output exposes {@code currentIndex} and {@code currentItem}
 * (when using loopOver) so that child tasks can reference them via
 * {@code ${loopTaskName.output.currentIndex}} and {@code ${loopTaskName.output.currentItem}}.
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

    static final String LOOP_OVER_VALUE_KEY = "loopOverValue";

    static final String INNER_LAST_TASK = "innerLastTask";

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
                    "Only one of 'loopOver' or 'loopCount' may be specified");
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

            // Evaluate expression if engine type is provided.
            Object evaluated = Optional.ofNullable(taskDefInput.getString(Options.ENGINE_TYPE))
                    .filter(StringUtils::isNotBlank)
                    .map(engineType ->
                            engineActuator.compute(
                                    engineType,
                                    String.valueOf(loopOverRaw),
                                    flowContext(context.getFlowInstance())
                            )
                    )
                    .orElse(loopOverRaw);

            if (evaluated instanceof List) {
                loopOverValue = (List<Object>) evaluated;
            } else {
                throw new IllegalArgumentException(
                        "The loop task 'loopOver' must evaluate to a list, got: "
                                + (evaluated == null ? "null" : evaluated.getClass().getName()));
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

        Map<String, Object> loopOutput = new HashMap<>();
        loopOutput.put(ITERATIONS_KEY, iterations);
        loopOutput.put(CURRENT_INDEX_KEY, 0);
        if (loopOverValue != null) {
            loopOutput.put(LOOP_OVER_VALUE_KEY, loopOverValue);
            if (!loopOverValue.isEmpty()) {
                loopOutput.put(CURRENT_ITEM_KEY, loopOverValue.get(0));
            }
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
            throw new IllegalStateException("LOOP task output must not be null when determining next task");
        }

        Map<String, Object> output = mappingTask.getOutput();

        int iterations = (int) output.get(ITERATIONS_KEY);
        int currentIndex = (int) output.get(CURRENT_INDEX_KEY);

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
            nextTask = loopBody.get(0);
        } else {
            // Find next task within the current iteration's loop body.
            nextTask = findNextTaskFromChildren(loopBody, target);

            // If we've exhausted the current iteration (MATCHED = last child completed),
            // advance to next iteration.
            if (nextTask == TaskDef.MATCHED) {
                int nextIndex = currentIndex + 1;
                if (nextIndex < iterations) {
                    // Move to next iteration.
                    output.put(CURRENT_INDEX_KEY, nextIndex);

                    // Update current item if loopOver mode.
                    List<Object> loopOverValue = (List<Object>) output.get(LOOP_OVER_VALUE_KEY);
                    if (loopOverValue != null && nextIndex < loopOverValue.size()) {
                        output.put(CURRENT_ITEM_KEY, loopOverValue.get(nextIndex));
                    }

                    // Return first task of loop body for next iteration.
                    nextTask = loopBody.get(0);
                }
                // else nextTask remains MATCHED, meaning all iterations done.
            }
        }

        if (nextTask != null && nextTask != TaskDef.MATCHED) {
            output.put(INNER_LAST_TASK, nextTask.getName());
        }

        return nextTask;
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

    @SuppressWarnings("all")
    private TaskDef findNextTaskFromChildren(final List<TaskDef> children, final TaskDef target) {
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
                        "Can be a list literal or an expression that evaluates to a list.");

        static final ConfigOption<Integer> LOOP_COUNT = ConfigOptions
                .key("loopCount")
                .intType()
                .noDefaultValue()
                .withDescription("The number of iterations to perform. " +
                        "Used when iterating a fixed number of times instead of over a collection.");

        static final ConfigOption<List<TaskDef>> LOOP_BODY = ConfigOptions
                .key("loopBody")
                .classType(TaskDef.class)
                .asList()
                .noDefaultValue()
                .withDescription("The list of child task definitions to execute for each iteration.");
    }
}
