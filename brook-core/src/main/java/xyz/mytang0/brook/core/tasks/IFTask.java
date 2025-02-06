package xyz.mytang0.brook.core.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
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

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static xyz.mytang0.brook.core.utils.ParameterUtils.flowContext;

public class IFTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("IF")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Title.");

    // Output keys.
    static final String HIT_INDEX_KEY = "hitIndex";

    static final String INNER_LAST_TASK = "innerLastTask";

    private final EngineActuator engineActuator;

    @Setter
    private FlowExecutor<?> flowExecutor;

    public IFTask() {
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
        options.add(Options.BRANCHES);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(Options.ENGINE_TYPE);
        return options;
    }

    @Override
    public List<TaskInstance> getMappedTasks(TaskMapperContext context) {

        Configuration taskDefInput = context.getInputConfiguration();

        List<Branch> branches = getBranches(context.getTaskDef());

        // Local hit index.
        int hitIndex = 0;
        List<TaskDef> hitTaskDefs = null;
        for (Branch branch : branches) {
            // Make sure the type of the condition result is boolean.
            Object object = Optional.ofNullable(taskDefInput.getString(Options.ENGINE_TYPE))
                    .filter(StringUtils::isNotBlank)
                    .map(engineType ->
                            engineActuator.compute(
                                    engineType,
                                    branch.getCondition(),
                                    flowContext(context.getFlowInstance())
                            )
                    )
                    .orElseGet(branch::getCondition);

            boolean result = object instanceof Boolean
                    ? (boolean) object
                    : BooleanUtils.toBoolean(String.valueOf(object));

            if (result) {
                hitTaskDefs = branch.getCases();
                break;
            }
            hitIndex++;
        }

        // Build if task self.
        TaskInstance ifTask = TaskInstance.create(context.getTaskDef());
        ifTask.setFlowId(context.getFlowInstance().getFlowId());
        ifTask.setInput(context.getInput());

        if (CollectionUtils.isNotEmpty(hitTaskDefs)) {
            Map<String, Object> ifOutput = new HashMap<>();
            ifOutput.put(HIT_INDEX_KEY, hitIndex);
            ifTask.setOutput(ifOutput);
        }

        return Collections.singletonList(ifTask);
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
            throw new IllegalStateException("When next, the IF task output null");
        }

        Map<String, Object> output = mappingTask.getOutput();

        int hitIndex = (int) output.get(HIT_INDEX_KEY);

        final List<Branch> branches = getBranches(toBeSearched);

        if (CollectionUtils.isEmpty(branches)) {
            return null;
        }

        // None match
        if (branches.size() <= hitIndex) {
            return null;
        }

        Branch mappingBranch = branches.get(hitIndex);

        TaskDef nextTask;

        if (toBeSearched == target
                || toBeSearched.getName().equals(target.getName())) {
            nextTask = mappingBranch.getCases().get(0);
        } else {
            nextTask = findNextTaskFromChildren(mappingBranch.getCases(), target);
        }

        if (nextTask != null && nextTask != TaskDef.MATCHED) {
            output.put(INNER_LAST_TASK, nextTask.getName());
        }

        return nextTask;
    }

    @SuppressWarnings("all")
    private List<Branch> getBranches(@NotNull final TaskDef taskdef) {
        List<Branch> branches = taskdef.getParsed();

        if (branches == null) {
            synchronized (taskdef) {

                branches = taskdef.getParsed();

                if (branches == null) {

                    if (!(taskdef.getInput() instanceof Map)) {
                        throw new IllegalArgumentException("The if task input type is not map");
                    }

                    branches = JsonUtils.convertValue(
                            ((Map<String, Object>) taskdef.getInput())
                                    .get(Options.BRANCHES.key()),
                            new TypeReference<List<Branch>>() {
                            }
                    );

                    taskdef.setParsed(branches);
                }
            }
        }
        return branches;
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
                .withDescription("The if branch condition expression evaluation engine type.");

        static final ConfigOption<List<Branch>> BRANCHES = ConfigOptions
                .key("branches")
                .classType(Branch.class)
                .asList()
                .noDefaultValue()
                .withDescription("The if all branches.");
    }

    @Data
    private static class Branch implements Serializable {

        private static final long serialVersionUID = 8412246263309887359L;

        private String condition;

        private List<TaskDef> cases;
    }
}
