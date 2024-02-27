package xyz.mytang0.brook.core.tasks;

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
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import xyz.mytang0.brook.core.utils.ParameterUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IFTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("IF")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Title.");

    private final EngineActuator engineActuator;

    private FlowExecutor<?> flowExecutor;

    public IFTask() {
        this.engineActuator = ExtensionDirector
                .getExtensionLoader(EngineActuator.class)
                .getDefaultExtension();
    }

    public void setFlowExecutor(FlowExecutor<?> flowExecutor) {
        this.flowExecutor = flowExecutor;
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
                                    ParameterUtils.flowContext(context.getFlowInstance())
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

        List<TaskInstance> scheduledTasks = new ArrayList<>();

        // Build if task self.
        TaskInstance ifTask = TaskInstance.create(context.getTaskDef());
        ifTask.setFlowId(context.getFlowInstance().getFlowId());
        ifTask.setInput(context.getInput());
        scheduledTasks.add(ifTask);

        if (CollectionUtils.isNotEmpty(hitTaskDefs)) {
            TaskDef scheduled = hitTaskDefs.get(0);
            IFOutput ifOutput = new IFOutput();
            ifOutput.setHitIndex(hitIndex);
            ifOutput.setInnerLastTask(scheduled.getName());
            ifTask.setOutput(ifOutput);

            scheduledTasks.addAll(
                    flowExecutor.getMappedTasks(
                            context.getFlowInstance(), scheduled)
            );
        }

        return scheduledTasks;
    }

    @Override
    public boolean execute(TaskInstance taskInstance) {
        taskInstance.setStatus(TaskStatus.COMPLETED);
        return true;
    }

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

        final List<Branch> branches = getBranches(toBeSearched);

        final FlowInstance currentFlow = FlowContext.getCurrentFlow();

        if (currentFlow != null) {
            Optional<TaskInstance> mappingTaskOptional =
                    currentFlow.getTaskByName(toBeSearched.getName());

            if (mappingTaskOptional.isPresent()) {

                TaskInstance mappingTask = mappingTaskOptional.get();

                if (mappingTask.getOutput() != null) {

                    IFOutput ifOutput = mappingTask.getOutput();

                    Branch mappingBranch = branches.get(ifOutput.getHitIndex());

                    TaskDef nextTask = findNextTaskFromChildren(mappingBranch.getCases(), target);

                    if (nextTask != null) {
                        ifOutput.setInnerLastTask(nextTask.getName());
                    }

                    return nextTask;
                }
            }
        }

        TaskDef nextTask = null;

        for (Branch branch : branches) {

            if (CollectionUtils.isEmpty(branch.getCases())) {
                break;
            }

            nextTask = findNextTaskFromChildren(branch.getCases(), target);
            if (nextTask != null) {
                break;
            }
        }

        return nextTask;
    }

    @SuppressWarnings("unchecked")
    private List<Branch> getBranches(final TaskDef taskdef) {
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

    private TaskDef findNextTaskFromChildren(final List<TaskDef> children, final TaskDef target) {
        Iterator<TaskDef> iterator = children.iterator();

        while (iterator.hasNext()) {
            TaskDef toBeSearchedSubTask = iterator.next();

            if (target.getName().equals(toBeSearchedSubTask.getName())) {
                break;
            }

            TaskDef nextTask = flowExecutor.getNextTask(toBeSearchedSubTask, target);
            if (nextTask != null) {
                return nextTask;
            }
        }

        if (iterator.hasNext()) {
            return iterator.next();
        }

        return null;
    }

    @Data
    static class IFOutput implements Serializable {

        private static final long serialVersionUID = -4008155033303592662L;

        private int hitIndex;

        private String innerLastTask;
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
