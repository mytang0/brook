package xyz.mytang0.brook.core.tasks;

import org.apache.commons.lang3.StringUtils;
import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.context.FlowContext;
import xyz.mytang0.brook.common.holder.UserHolder;
import xyz.mytang0.brook.common.metadata.extension.Extension;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.metadata.model.StartFlowReq;
import xyz.mytang0.brook.common.metadata.model.User;
import xyz.mytang0.brook.core.FlowExecutor;
import xyz.mytang0.brook.spi.task.FlowTask;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SubFlowTask implements FlowTask {

    public static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("SUB_FLOW")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("The subflow task, a sub-flow can be started through it. " +
                    "Can be used for flow reuse");

    private FlowExecutor<?> flowExecutor;

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
        options.add(Options.FLOW_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(Options.FLOW_VERSION);
        options.add(Options.FLOW_INPUT);
        options.add(Options.CORRELATION_ID);
        options.add(Options.EXTENSION);
        return options;
    }

    @Override
    public boolean execute(TaskInstance taskInstance) {
        if (StringUtils.isNotBlank(taskInstance.getSubFlowId())) {
            return false;
        }

        Configuration input = taskInstance.getInputConfiguration();

        User currentUser = UserHolder.getCurrentUser();

        UserHolder.setCurrentUser(FlowContext.getCurrentFlow().getCreator());

        try {
            String subFlowId = flowExecutor.startFlow(StartFlowReq
                    .builder()
                    .name(input.get(Options.FLOW_NAME))
                    .version(input.get(Options.FLOW_VERSION))
                    .input(input.get(Options.FLOW_INPUT))
                    .correlationId(input.get(Options.CORRELATION_ID))
                    .extension(input.get(Options.EXTENSION))
                    .parentFlowId(taskInstance.getFlowId())
                    .parentTaskId(taskInstance.getTaskId())
                    .build());

            taskInstance.setSubFlowId(subFlowId);
        } finally {
            UserHolder.clearCurrentUser();
            if (currentUser != null) {
                UserHolder.setCurrentUser(currentUser);
            }
        }
        return true;
    }

    @Override
    public void cancel(TaskInstance taskInstance) {
        if (StringUtils.isBlank(taskInstance.getSubFlowId())) {
            return;
        }

        flowExecutor.terminate(
                taskInstance.getSubFlowId(),
                "Cancel");
    }

    public static class Options {

        public static final ConfigOption<String> FLOW_NAME = ConfigOptions
                .key("flowName")
                .stringType()
                .noDefaultValue()
                .withDescription("The subflow name.");

        public static final ConfigOption<Integer> FLOW_VERSION = ConfigOptions
                .key("flowVersion")
                .intType()
                .defaultValue(0)
                .withDescription("The subflow version.");

        public static final ConfigOption<Object> FLOW_INPUT = ConfigOptions
                .key("flowInput")
                .classType(Object.class)
                .noDefaultValue()
                .withDescription("The subflow input.");

        public static final ConfigOption<String> CORRELATION_ID = ConfigOptions
                .key("correlationId")
                .stringType()
                .noDefaultValue()
                .withDescription("The correlation id.");

        public static final ConfigOption<Extension> EXTENSION = ConfigOptions
                .key("extension")
                .classType(Extension.class)
                .noDefaultValue()
                .withDescription("The extension.");
    }
}
