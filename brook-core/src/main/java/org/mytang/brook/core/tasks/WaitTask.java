package org.mytang.brook.core.tasks;

import org.mytang.brook.common.configuration.ConfigOption;
import org.mytang.brook.common.configuration.ConfigOptions;
import org.mytang.brook.common.metadata.instance.TaskInstance;
import org.mytang.brook.spi.task.FlowTask;

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
