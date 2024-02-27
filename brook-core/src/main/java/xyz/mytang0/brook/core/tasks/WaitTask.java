package xyz.mytang0.brook.core.tasks;

import xyz.mytang0.brook.common.configuration.ConfigOption;
import xyz.mytang0.brook.common.configuration.ConfigOptions;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.spi.task.FlowTask;

import java.util.Map;

public class WaitTask implements FlowTask {

    static final ConfigOption<Map<String, Object>> CATALOG = ConfigOptions
            .key("WAIT")
            .classType(PROPERTIES_MAP_CLASS)
            .noDefaultValue()
            .withDescription("Suspend the task and wait for external execution.");

    @Override
    public ConfigOption<?> catalog() {
        return CATALOG;
    }

    @Override
    public boolean execute(TaskInstance taskInstance) {
        return false;
    }
}
