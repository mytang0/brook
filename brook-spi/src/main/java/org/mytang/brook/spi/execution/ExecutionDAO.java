package org.mytang.brook.spi.execution;


import org.mytang.brook.common.extension.SPI;
import org.mytang.brook.common.metadata.instance.FlowInstance;
import org.mytang.brook.common.metadata.instance.TaskInstance;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

@SPI
public interface ExecutionDAO {

    void createFlow(FlowInstance flowInstance);

    void updateFlow(FlowInstance flowInstance);

    void deleteFlow(String flowId);

    FlowInstance getFlow(String flowId);

    default FlowInstance getFlowByCorrelationId(String correlationId) {
        return null;
    }

    List<String> getRunningFlowIds(String flowName);

    List<TaskInstance> createTasks(List<TaskInstance> taskInstances);

    default void updateTasks(List<TaskInstance> taskInstances) {
        if (CollectionUtils.isNotEmpty(taskInstances)) {
            taskInstances.forEach(this::updateTask);
        }
    }

    void updateTask(TaskInstance taskInstance);

    void deleteTask(String taskId);

    TaskInstance getTask(String taskId);

    TaskInstance getTaskByName(String flowId, String taskName);

    List<String> getRunningTaskIds(String taskName);

    List<TaskInstance> getTasksForFlow(String flowId);
}
