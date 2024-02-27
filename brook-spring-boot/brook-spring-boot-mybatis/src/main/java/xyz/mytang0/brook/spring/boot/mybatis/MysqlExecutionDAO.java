package xyz.mytang0.brook.spring.boot.mybatis;

import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.spi.annotation.FlowSelectedSPI;
import xyz.mytang0.brook.spi.execution.ExecutionDAO;
import xyz.mytang0.brook.spring.boot.mybatis.entity.Flow;
import xyz.mytang0.brook.spring.boot.mybatis.entity.FlowPending;
import xyz.mytang0.brook.spring.boot.mybatis.entity.Task;
import xyz.mytang0.brook.spring.boot.mybatis.entity.TaskPending;
import xyz.mytang0.brook.spring.boot.mybatis.mapper.FlowMapper;
import xyz.mytang0.brook.spring.boot.mybatis.mapper.FlowPendingMapper;
import xyz.mytang0.brook.spring.boot.mybatis.mapper.TaskMapper;
import xyz.mytang0.brook.spring.boot.mybatis.mapper.TaskPendingMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
@FlowSelectedSPI(name = "mysql")
@ConditionalOnProperty(name = "brook.execution-dao.mysql.enabled", havingValue = "true")
public class MysqlExecutionDAO implements ExecutionDAO {

    @Resource
    private FlowMapper flowMapper;

    @Resource
    private TaskMapper taskMapper;

    @Resource
    private FlowPendingMapper flowPendingMapper;

    @Resource
    private TaskPendingMapper taskPendingMapper;

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public void createFlow(FlowInstance flowInstance) {
        // Avoid redundant 'taskInstances'.
        final List<TaskInstance> taskInstances =
                flowInstance.getTaskInstances();
        flowInstance.setTaskInstances(Collections.emptyList());

        flowMapper.insert(convert(flowInstance));

        flowInstance.setTaskInstances(taskInstances);

        createFlowPending(flowInstance);
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public void updateFlow(FlowInstance flowInstance) {
        // Avoid redundant 'taskInstances'.
        final List<TaskInstance> taskInstances =
                flowInstance.getTaskInstances();
        flowInstance.setTaskInstances(Collections.emptyList());

        flowMapper.updateByFlowId(convert(flowInstance));

        flowInstance.setTaskInstances(taskInstances);

        if (flowInstance.getStatus().isTerminal()) {
            deleteFlowPending(flowInstance.getFlowId());
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public void deleteFlow(String flowId) {
        Flow flow = flowMapper.selectByFlowId(flowId);

        if (flow == null) {
            log.warn("No such flow found by id {}", flowId);
            return;
        }

        deleteFlowPending(flowId);
        deleteTasksByFlowId(flowId);

        flowMapper.deleteById(flow.getId());
    }

    private void deleteTasksByFlowId(String flowId) {
        taskMapper.deleteByFlowId(flowId);

        taskPendingMapper.deleteByFlowId(flowId);
    }

    @Override
    public FlowInstance getFlow(String flowId) {
        Flow flow = flowMapper.selectByFlowId(flowId);

        FlowInstance flowInstance = Optional.ofNullable(flow)
                .map(this::convert).orElse(null);

        if (flowInstance != null) {
            flowInstance.setTaskInstances(
                    getTasksForFlow(flowId)
            );
        }

        return flowInstance;
    }

    @Override
    public FlowInstance getFlowByCorrelationId(String correlationId) {
        Flow flow = flowMapper.selectByCorrelationId(correlationId);

        FlowInstance flowInstance = Optional.ofNullable(flow)
                .map(this::convert).orElse(null);

        if (flowInstance != null) {
            flowInstance.setTaskInstances(
                    getTasksForFlow(flow.getFlowId())
            );
        }

        return flowInstance;
    }

    @Override
    public List<String> getRunningFlowIds(String flowName) {
        return flowPendingMapper.selectFlowIds(flowName);
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public List<TaskInstance> createTasks(List<TaskInstance> taskInstances) {
        if (CollectionUtils.isEmpty(taskInstances)) {
            return taskInstances;
        }

        List<TaskInstance> createdTasks =
                new ArrayList<>(taskInstances.size());

        taskInstances.forEach(taskInstance -> {

            Task task = convert(taskInstance);

            task.setJsonDef(JsonUtils.toJsonString(
                    taskInstance.getTaskDef()));

            if (0 < taskMapper.insert(task)) {
                createTaskPending(taskInstance);

                createdTasks.add(taskInstance);
            }
        });

        return createdTasks;
    }

    @Override
    public void updateTask(TaskInstance taskInstance) {
        taskMapper.updateByTaskId(convert(taskInstance));

        if (taskInstance.getStatus().isTerminal()) {
            deleteTaskPending(taskInstance.getTaskId());
        }
    }

    @Transactional(rollbackFor = Throwable.class)
    @Override
    public void deleteTask(String taskId) {
        Task task = taskMapper.selectByTaskId(taskId);

        if (task == null) {
            log.warn("No such task found by id {}", taskId);
            return;
        }

        deleteTaskPending(taskId);
        taskMapper.deleteById(task.getId());
    }

    @Override
    public TaskInstance getTask(String taskId) {
        Task task = taskMapper.selectByTaskId(taskId);

        return Optional.ofNullable(task)
                .map(this::convert).orElse(null);
    }

    @Override
    public TaskInstance getTaskByName(String flowId, String taskName) {
        Task task = taskMapper.selectByTaskName(flowId, taskName);

        return Optional.ofNullable(task)
                .map(this::convert).orElse(null);
    }

    @Override
    public List<String> getRunningTaskIds(String taskName) {
        return taskPendingMapper.selectTaskIds(taskName);
    }

    @Override
    public List<TaskInstance> getTasksForFlow(String flowId) {
        List<Task> tasks = taskMapper.selectByFlowId(flowId);

        if (CollectionUtils.isNotEmpty(tasks)) {
            return tasks.stream().map(this::convert)
                    .collect(Collectors.toList());
        }

        return Collections.emptyList();
    }

    public Flow convert(FlowInstance flowInstance) {
        Flow flow = new Flow();
        flow.setFlowId(flowInstance.getFlowId());
        flow.setFlowName(flowInstance.getFlowName());
        flow.setFlowVersion(flowInstance.getFlowVersion());
        flow.setCorrelationId(flowInstance.getCorrelationId());
        flow.setJsonData(JsonUtils.toJsonString(flowInstance));
        return flow;
    }

    public Task convert(TaskInstance taskInstance) {
        Task task = new Task();
        task.setTaskId(taskInstance.getTaskId());
        task.setTaskName(taskInstance.getTaskName());
        task.setFlowId(taskInstance.getFlowId());
        TaskDef taskDef = taskInstance.getTaskDef();
        // Avoid redundancy.
        taskInstance.setTaskDef(null);
        task.setJsonData(JsonUtils.toJsonString(taskInstance));
        taskInstance.setTaskDef(taskDef);
        return task;
    }

    private FlowInstance convert(Flow flow) {
        return JsonUtils.readValue(flow.getJsonData(), FlowInstance.class);
    }

    private TaskInstance convert(Task task) {
        TaskInstance taskInstance =
                JsonUtils.readValue(task.getJsonData(), TaskInstance.class);
        if (Objects.isNull(taskInstance.getTaskDef())) {
            if (StringUtils.isNotBlank(task.getJsonDef())) {
                taskInstance.setTaskDef(
                        JsonUtils.readValue(task.getJsonDef(), TaskDef.class)
                );
            }
        }
        return taskInstance;
    }

    private void createFlowPending(FlowInstance flowInstance) {
        FlowPending flowPending = new FlowPending();
        flowPending.setFlowId(flowInstance.getFlowId());
        flowPending.setFlowName(flowInstance.getFlowName());

        flowPendingMapper.insert(flowPending);
    }

    private void deleteFlowPending(String flowId) {
        flowPendingMapper.deleteByFlowId(flowId);
    }

    private void createTaskPending(TaskInstance taskInstance) {
        TaskPending taskPending = new TaskPending();
        taskPending.setFlowId(taskInstance.getFlowId());
        taskPending.setTaskId(taskInstance.getTaskId());
        taskPending.setTaskName(taskInstance.getTaskName());

        taskPendingMapper.insert(taskPending);
    }

    private void deleteTaskPending(String taskId) {
        taskPendingMapper.deleteByTaskId(taskId);
    }
}
