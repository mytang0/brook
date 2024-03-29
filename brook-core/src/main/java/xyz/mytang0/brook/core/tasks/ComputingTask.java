package xyz.mytang0.brook.core.tasks;

import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.context.FlowContext;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.StringUtils;
import xyz.mytang0.brook.spi.computing.EngineActuator;
import xyz.mytang0.brook.spi.oss.OSSStorage;
import xyz.mytang0.brook.spi.task.FlowTask;
import xyz.mytang0.brook.core.utils.ParameterUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ComputingTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("COMPUTING")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Engine task, " +
                    "select a supported compute engine for computational logic, " +
                    "the context is flowContext.");

    private final EngineActuator engineActuator;

    public ComputingTask() {
        this.engineActuator = ExtensionDirector
                .getExtensionLoader(EngineActuator.class)
                .getDefaultExtension();
    }

    @Override
    public ConfigOption<?> catalog() {
        return CATALOG;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(Options.ENGINE_TYPE);
        options.add(Options.SOURCE);
        options.add(Options.STORAGE);
        options.add(Options.PARAMS);
        return options;
    }

    @Override
    public boolean execute(TaskInstance taskInstance) {
        Configuration input = taskInstance.getInputConfiguration();
        taskInstance.setInput(null);

        Map<String, Object> params = input.get(Options.PARAMS);
        if (Objects.isNull(params)) {
            params = ParameterUtils.flowContext(FlowContext.getCurrentFlow());
            params.remove(taskInstance.getTaskName());
        }

        String engineType = input.get(Options.ENGINE_TYPE);
        String source = input.get(Options.SOURCE);
        Object output = null;

        if (StringUtils.isBlank(source)) {
            OSSStorage storage = input.get(Options.STORAGE);
            if (Objects.nonNull(storage)) {
                output = engineActuator.compute(engineType, storage, params);
            }
        } else {
            output = engineActuator.compute(engineType, source, params);
        }

        taskInstance.setOutput(output);
        taskInstance.setStatus(TaskStatus.COMPLETED);
        return true;
    }

    static class Options {

        static final ConfigOption<String> ENGINE_TYPE = ConfigOptions
                .key("engineType")
                .stringType()
                .defaultValue("noop")
                .withDescription("The selected compute engine type.");

        static final ConfigOption<String> SOURCE = ConfigOptions
                .key("source")
                .stringType()
                .noDefaultValue()
                .withDescription("The computational logic.");

        static final ConfigOption<OSSStorage> STORAGE = ConfigOptions
                .key("storage")
                .classType(OSSStorage.class)
                .noDefaultValue()
                .withDescription("The computational logic oss storage information.");

        @SuppressWarnings("unchecked")
        static final ConfigOption<Map<String, Object>> PARAMS = ConfigOptions
                .key("params")
                .classType((Class<Map<String, Object>>) (Class<?>) Map.class)
                .noDefaultValue()
                .withDescription("The computational logic params.");
    }
}
