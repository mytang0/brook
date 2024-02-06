package org.mytang.brook.spi.task;

import org.mytang.brook.common.extension.SPI;
import org.mytang.brook.common.utils.JsonUtils;
import org.mytang.brook.common.utils.TimeUtils;
import org.mytang.brook.common.context.TaskMapperContext;
import org.mytang.brook.common.metadata.definition.TaskDef;
import org.mytang.brook.common.metadata.enums.TaskStatus;
import org.mytang.brook.common.metadata.instance.TaskInstance;
import com.fasterxml.jackson.core.type.TypeReference;

import javax.validation.constraints.NotBlank;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SPI
public interface FlowTask extends Options {

    default @NotBlank String getType() {
        return catalog().key();
    }

    default Object getInput(Object external) {
        return JsonUtils.convertValue(external,
                new TypeReference<Map<String, Object>>() {
                });
    }

    default List<TaskInstance> getMappedTasks(final TaskMapperContext context) {

        final TaskInstance taskInstance = TaskInstance.create(context.getTaskDef());
        taskInstance.setFlowId(context.getFlowInstance().getFlowId());
        taskInstance.setInput(context.getInput());

        return Collections.singletonList(taskInstance);
    }

    boolean execute(final TaskInstance taskInstance);

    default void cancel(final TaskInstance taskInstance) {
        if (!taskInstance.getStatus().isTerminal()) {
            taskInstance.setStatus(TaskStatus.CANCELED);
            taskInstance.setEndTime(TimeUtils.currentTimeMillis());
        }
    }

    default TaskDef next(final TaskDef toBeSearched, final TaskDef target) {
        if (!getType().equals(toBeSearched.getType())) {
            throw new IllegalArgumentException(
                    String.format("The 'next' method cannot be executed, " +
                                    "because the to be searched task type does not match, %s != %s",
                            getType(), toBeSearched.getType()));
        }

        if (target == null) {
            throw new IllegalArgumentException("The target task is null");
        }

        return null;
    }
}
