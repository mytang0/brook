package xyz.mytang0.brook.core.tasks;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;
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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static xyz.mytang0.brook.core.utils.ParameterUtils.flowContext;

public class SwitchTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("SWITCH")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Branch task, used to fork flow according to conditions.");

    private final EngineActuator engineActuator;

    @Setter
    private FlowExecutor<?> flowExecutor;

    public SwitchTask() {
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
        options.add(Options.CASE);
        options.add(Options.DECISION_CASES);
        options.add(Options.DEFAULT_CASE_KEY);
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

        // Make sure the type of the branch key is String.
        String matchedCaseKey = String.valueOf(
                Optional.ofNullable(taskDefInput.getString(Options.ENGINE_TYPE))
                        .filter(StringUtils::isNotBlank)
                        .map(engineType ->
                                engineActuator.compute(
                                        engineType,
                                        taskDefInput.getString(Options.CASE),
                                        flowContext(context.getFlowInstance())
                                )
                        )
                        .orElseGet(() -> taskDefInput.getString(Options.CASE))
        );

        Map<String, List<TaskDef>> decisionCases = getDecisionCases(context.getTaskDef());

        // When no match is found, go to the default branch.
        if (StringUtils.isBlank(matchedCaseKey) ||
                !decisionCases.containsKey(matchedCaseKey)) {
            matchedCaseKey = taskDefInput.getString(Options.DEFAULT_CASE_KEY);
        }

        // Build switch task self.
        TaskInstance switchTask = TaskInstance.create(context.getTaskDef());
        switchTask.setFlowId(context.getFlowInstance().getFlowId());
        switchTask.setInput(context.getInput());
        SwitchOutput switchOutput = new SwitchOutput();
        switchOutput.setMatchedCaseKey(matchedCaseKey);
        switchTask.setOutput(switchOutput);

        return Collections.singletonList(switchTask);
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
            throw new IllegalStateException("When next, the SWITCH task output null");
        }

        SwitchOutput switchOutput = JsonUtils.convertValue(
                mappingTask.getOutput(), SwitchOutput.class);

        final Map<String, List<TaskDef>> decisionCases =
                getDecisionCases(toBeSearched);

        List<TaskDef> mappingTaskDefs = decisionCases.get(
                switchOutput.getMatchedCaseKey()
        );

        if (CollectionUtils.isEmpty(mappingTaskDefs)) {
            return null;
        }

        TaskDef nextTask;

        if (toBeSearched == target
                || toBeSearched.getName().equals(target.getName())) {
            nextTask = mappingTaskDefs.get(0);
        } else {
            nextTask = findNextTaskFromChildren(mappingTaskDefs, target);
        }

        if (nextTask != null && nextTask != TaskDef.MATCHED) {
            switchOutput.setInnerLastTask(nextTask.getName());
        }

        return nextTask;
    }

    @SuppressWarnings("all")
    private Map<String, List<TaskDef>> getDecisionCases(final TaskDef taskdef) {
        Map<String, List<TaskDef>> decisionCases = taskdef.getParsed();
        if (decisionCases == null) {
            synchronized (taskdef) {

                decisionCases = taskdef.getParsed();

                if (decisionCases == null) {

                    if (!(taskdef.getInput() instanceof Map)) {
                        throw new IllegalArgumentException("The switch task input type is not map");
                    }

                    decisionCases = JsonUtils.convertValue(
                            ((Map<String, Object>) taskdef.getInput())
                                    .get(Options.DECISION_CASES.key()),
                            new TypeReference<Map<String, List<TaskDef>>>() {
                            });

                    taskdef.setParsed(decisionCases);
                }
            }
        }
        return decisionCases;
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

    @Data
    static class SwitchOutput implements Serializable {

        private static final long serialVersionUID = -3010667025859390068L;

        private String matchedCaseKey;

        private String innerLastTask;
    }

    static class Options {

        static final ConfigOption<String> ENGINE_TYPE = ConfigOptions
                .key("engineType")
                .stringType()
                .noDefaultValue()
                .withDescription("The switch branch condition expression evaluation engine type.");

        static final ConfigOption<String> CASE = ConfigOptions
                .key("case")
                .stringType()
                .noDefaultValue()
                .withDescription("The switch branch case expression," +
                        " the result is the key of the branch map {@link decisionCases}.");

        static final ConfigOption<Map<String, Object>> DECISION_CASES = ConfigOptions
                .key("decisionCases")
                .classType(PROPERTIES_MAP_CLASS)
                .noDefaultValue()
                .withDescription("The switch branch map {@link Map<String, List<TaskDef>>}.");

        static final ConfigOption<String> DEFAULT_CASE_KEY = ConfigOptions
                .key("defaultCaseKey")
                .stringType()
                .noDefaultValue()
                .withDescription("The default branch key when the computed result does not match.");
    }
}
