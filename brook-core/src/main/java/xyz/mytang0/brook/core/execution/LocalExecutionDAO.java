package xyz.mytang0.brook.core.execution;

import xyz.mytang0.brook.common.extension.Disposable;
import xyz.mytang0.brook.common.extension.Selected;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.spi.execution.ExecutionDAO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Selected
public class LocalExecutionDAO implements ExecutionDAO, Disposable {

    private final Map<String, FlowInstance> flowInstances
            = new ConcurrentHashMap<>();

    private final Map<String, TaskInstance> taskInstances
            = new ConcurrentHashMap<>();

    @Override
    public void createFlow(FlowInstance flowInstance) {
        flowInstances.put(flowInstance.getFlowId(), flowInstance);
    }

    @Override
    public void updateFlow(FlowInstance flowInstance) {
        if (flowInstance.getStatus().isTerminal()) {
            deleteFlow(flowInstance.getFlowId());
        } else {
            flowInstances.put(
                    flowInstance.getFlowId(), flowInstance);
        }
    }

    @Override
    public void deleteFlow(String flowId) {
        Optional.ofNullable(flowInstances.remove(flowId))
                .ifPresent(flow -> {
                    if (CollectionUtils.isNotEmpty(flow.getTaskInstances())) {
                        flow.getTaskInstances().forEach(task ->
                                taskInstances.remove(task.getTaskId())
                        );
                    }
                });
    }

    @Override
    public FlowInstance getFlow(String flowId) {
        return flowInstances.get(flowId);
    }

    @Override
    public List<TaskInstance> getTasksForFlow(String flowId) {
        return Optional.ofNullable(flowInstances.get(flowId))
                .map(FlowInstance::getTaskInstances)
                .orElse(Collections.emptyList());
    }

    @Override
    public List<String> getRunningFlowIds(String flowName) {
        // Do not support
        return Collections.emptyList();
    }

    @Override
    public List<TaskInstance> createTasks(List<TaskInstance> taskInstances) {
        taskInstances.forEach(this::createTask);
        return taskInstances;
    }

    private void createTask(TaskInstance taskInstance) {
        taskInstances.put(taskInstance.getTaskId(), taskInstance);
    }

    @Override
    public void updateTask(TaskInstance taskInstance) {
        taskInstances.computeIfPresent(taskInstance.getTaskId(),
                (taskId, instance) -> taskInstance);
    }

    @Override
    public void deleteTask(String taskId) {
        Optional.ofNullable(taskInstances.remove(taskId))
                .flatMap(taskInstance ->
                        Optional.ofNullable(
                                flowInstances.get(
                                        taskInstance.getFlowId())))
                .ifPresent(flowInstance ->
                        flowInstance.setTaskInstances(
                                flowInstance.getTaskInstances()
                                        .stream()
                                        .filter(taskInstance ->
                                                !StringUtils.equals(
                                                        taskInstance.getTaskId(),
                                                        taskId))
                                        .collect(Collectors.toList())
                        )
                );
    }

    @Override
    public TaskInstance getTask(String taskId) {
        return taskInstances.get(taskId);
    }

    @Override
    public TaskInstance getTaskByName(String flowId, String taskName) {
        return Optional.ofNullable(flowInstances.get(flowId))
                .flatMap(flowInstance ->
                        flowInstance.getTaskByName(taskName))
                .orElse(null);
    }

    @Override
    public List<String> getRunningTaskIds(String taskName) {
        // Do not support
        return Collections.emptyList();
    }

    @Override
    public void destroy() {
        taskInstances.clear();
        flowInstances.clear();
    }
}
