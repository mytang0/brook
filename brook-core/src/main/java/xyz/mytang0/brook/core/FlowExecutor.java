package xyz.mytang0.brook.core;

import com.google.common.base.Joiner;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.constants.Delimiter;
import xyz.mytang0.brook.common.context.FlowContext;
import xyz.mytang0.brook.common.context.TaskMapperContext;
import xyz.mytang0.brook.common.exception.BizException;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.enums.FlowStatus;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import xyz.mytang0.brook.common.metadata.instance.TaskInstance;
import xyz.mytang0.brook.common.metadata.model.QueueMessage;
import xyz.mytang0.brook.common.metadata.model.SkipTaskReq;
import xyz.mytang0.brook.common.metadata.model.StartFlowReq;
import xyz.mytang0.brook.common.metadata.model.TaskResult;
import xyz.mytang0.brook.common.utils.ExceptionUtils;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.common.utils.TimeUtils;
import xyz.mytang0.brook.core.aspect.FlowAspect;
import xyz.mytang0.brook.core.aspect.TaskAspect;
import xyz.mytang0.brook.core.exception.FlowException;
import xyz.mytang0.brook.core.exception.TerminateException;
import xyz.mytang0.brook.core.execution.ExecutionProperties;
import xyz.mytang0.brook.core.lock.FlowLockFacade;
import xyz.mytang0.brook.core.lock.LockProperties;
import xyz.mytang0.brook.core.metadata.MetadataFacade;
import xyz.mytang0.brook.core.metadata.MetadataProperties;
import xyz.mytang0.brook.core.monitor.DelayedTaskMonitor;
import xyz.mytang0.brook.core.monitor.DelayedTaskMonitorProperties;
import xyz.mytang0.brook.core.queue.QueueProperties;
import xyz.mytang0.brook.spi.cache.FlowCache;
import xyz.mytang0.brook.spi.cache.FlowCacheFactory;
import xyz.mytang0.brook.spi.computing.EngineActuator;
import xyz.mytang0.brook.spi.config.ConfiguratorFacade;
import xyz.mytang0.brook.spi.execution.ExecutionDAO;
import xyz.mytang0.brook.spi.executor.ExecutorFactory;
import xyz.mytang0.brook.spi.metadata.MetadataService;
import xyz.mytang0.brook.spi.queue.QueueService;
import xyz.mytang0.brook.spi.task.FlowTask;

import javax.validation.ValidationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static xyz.mytang0.brook.core.constants.FlowConstants.DEFAULT_TIMEOUT_MS;
import static xyz.mytang0.brook.core.constants.FlowConstants.LOCK_TRY_TIME_MS;
import static xyz.mytang0.brook.core.exception.FlowErrorCode.CONCURRENCY_LIMIT;
import static xyz.mytang0.brook.core.exception.FlowErrorCode.FLOW_EXECUTION_CONFLICT;
import static xyz.mytang0.brook.core.exception.FlowErrorCode.FLOW_EXECUTION_ERROR;
import static xyz.mytang0.brook.core.exception.FlowErrorCode.FLOW_NOT_EXIST;
import static xyz.mytang0.brook.core.exception.FlowErrorCode.TASK_NOT_EXIST;
import static xyz.mytang0.brook.core.executor.ExecutorEnum.ASYNC_EXECUTOR;
import static xyz.mytang0.brook.core.executor.ExecutorEnum.FLOW_STARTER;
import static xyz.mytang0.brook.core.utils.ParameterUtils.flowContext;
import static xyz.mytang0.brook.core.utils.ParameterUtils.getFlowInput;
import static xyz.mytang0.brook.core.utils.ParameterUtils.getFlowOutput;
import static xyz.mytang0.brook.core.utils.ParameterUtils.getMappingValue;
import static xyz.mytang0.brook.core.utils.ParameterUtils.getTaskInput;
import static xyz.mytang0.brook.core.utils.ParameterUtils.getTaskOutput;
import static xyz.mytang0.brook.core.utils.QueueUtils.getTaskDelayQueueName;

@Slf4j
public class FlowExecutor<T extends FlowTask> {

    private static final Map<String, RequestCallback>
            requestCallbacks = new ConcurrentHashMap<>();

    private final MetadataService metadataService;

    private final FlowTaskRegistry<T> flowTaskRegistry;

    private final FlowLockFacade flowLockFacade;

    private final EngineActuator engineActuator;

    private final ExecutorService flowStarter;

    private final ExecutorService asyncExecutor;

    private final FlowCacheFactory flowCacheFactory;

    private final FlowAspect flowAspect;

    private final TaskAspect taskAspect;

    private final QueueProperties queueProperties;

    private final ExecutionProperties executionProperties;

    private final DelayedTaskMonitorProperties delayedTaskMonitorProperties;


    public FlowExecutor(FlowTaskRegistry<T> flowTaskRegistry) {
        this.flowLockFacade = new FlowLockFacade(
                ConfiguratorFacade.getConfig(LockProperties.class)
        );
        this.flowTaskRegistry = flowTaskRegistry;
        this.flowAspect = new FlowAspect();
        this.taskAspect = new TaskAspect();
        this.metadataService = new MetadataFacade(
                ConfiguratorFacade.getConfig(MetadataProperties.class)
        );
        this.engineActuator = ExtensionDirector
                .getExtensionLoader(EngineActuator.class)
                .getDefaultExtension();
        this.flowCacheFactory = ExtensionDirector
                .getExtensionLoader(FlowCacheFactory.class)
                .getDefaultExtension();
        this.flowStarter = ExtensionDirector
                .getExtensionLoader(ExecutorFactory.class)
                .getDefaultExtension()
                .getExecutor(FLOW_STARTER);
        this.asyncExecutor = ExtensionDirector
                .getExtensionLoader(ExecutorFactory.class)
                .getDefaultExtension()
                .getExecutor(ASYNC_EXECUTOR);
        this.queueProperties =
                ConfiguratorFacade.getConfig(QueueProperties.class);
        this.executionProperties =
                ConfiguratorFacade.getConfig(ExecutionProperties.class);
        this.delayedTaskMonitorProperties =
                ConfiguratorFacade.getConfig(DelayedTaskMonitorProperties.class);

        DelayedTaskMonitor.init(this, flowLockFacade, delayedTaskMonitorProperties);
    }

    public String startFlow(StartFlowReq startFlowReq) {
        if (startFlowReq.getFlowDef() == null) {
            requireNonNull(startFlowReq.getName(),
                    "The 'flowName' is null");
            startFlowReq.setFlowDef(
                    metadataService.getFlow(
                            startFlowReq.getName(),
                            startFlowReq.getVersion()));
        }
        requireNonNull(startFlowReq.getFlowDef(),
                String.format("The flowDef of '%s' not exist",
                        startFlowReq.getName()));

        FlowInstance flowInstance =
                newFlowInstance(startFlowReq);

        checkConcurrency(flowInstance);

        createFlow(flowInstance);

        flowStarter.execute(() -> execute(flowInstance));

        return flowInstance.getFlowId();
    }

    public FlowInstance requestFlow(StartFlowReq startFlowReq) {
        long beginTimestampMs = TimeUtils.currentTimeMillis();

        if (startFlowReq.getFlowDef() == null) {
            requireNonNull(startFlowReq.getName(),
                    "The 'flowName' is null");
            startFlowReq.setFlowDef(
                    metadataService.getFlow(
                            startFlowReq.getName(),
                            startFlowReq.getVersion()));
        }
        requireNonNull(startFlowReq.getFlowDef(),
                String.format("The flowDef of '%s' not exist",
                        startFlowReq.getName()));

        FlowInstance flowInstance =
                newFlowInstance(startFlowReq);

        try {
            createFlow(flowInstance);

            final CountDownLatch terminalLatch
                    = new CountDownLatch(1);

            requestCallbacks.putIfAbsent(
                    flowInstance.getFlowId(),
                    terminalLatch::countDown);

            flowStarter.execute(() -> execute(flowInstance));

            long timeoutMs = DEFAULT_TIMEOUT_MS;

            FlowDef.ControlDef controlDef =
                    flowInstance.getFlowDef().getControlDef();
            if (controlDef != null) {
                if (controlDef.getTimeoutMs() > 0) {
                    timeoutMs = controlDef.getTimeoutMs();
                }
            }

            if (!terminalLatch.await(
                    remainWaitTime(timeoutMs, beginTimestampMs),
                    TimeUnit.MILLISECONDS)) {
                flowInstance.setStatus(FlowStatus.TIMED_OUT);
            }
        } catch (RejectedExecutionException re) {
            flowInstance.setStatus(FlowStatus.FAILED);
            flowInstance.setReasonForNotCompleting(
                    "The system is busy and refuses to execute!");
        } catch (Throwable throwable) {
            flowInstance.setStatus(FlowStatus.FAILED);
            flowInstance.setReasonForNotCompleting(
                    ExceptionUtils.getMessage(throwable));
        } finally {
            try {
                requestCallbacks.remove(
                        flowInstance.getFlowId());
                getExecutionDAO(flowInstance)
                        .deleteFlow(flowInstance.getFlowId());
            } catch (Throwable throwable) {
                log.warn("Request-level flow failed to delete flow instance: {}",
                        flowInstance.getFlowId());
            }
        }

        return flowInstance;
    }

    public void executeTask(String taskId) {
        TaskInstance taskInstance = getTask(taskId);
        if (taskInstance == null) {
            log.warn("Execute taskId: {}, " +
                    "but no such task found.", taskId);
            return;
        }

        if (taskInstance.getStatus().isFinished()) {
            return;
        } else if (taskInstance.isHanging()) {
            TaskInstance parentTaskInstance
                    = getTask(taskInstance.getParentTaskId());
            if (parentTaskInstance == null
                    || !parentTaskInstance.getStatus().isHanged()) {
                return;
            }
        }

        FlowInstance flowInstance =
                getFlow(taskInstance.getFlowId());
        if (flowInstance == null) {
            log.warn("Execute taskId: {}, " +
                            "but no such flow: {} found.",
                    taskId,
                    taskInstance.getFlowId());
            return;
        }

        // Avoid contaminating the original instance.
        flowInstance = flowInstance.deepCopy();

        // Second Confirm.
        Optional<TaskInstance> taskOptional =
                flowInstance.getTaskById(taskId);
        if (!taskOptional.isPresent()) {
            log.warn("Execute taskId: {}, " +
                    "but no such task found.", taskId);
            return;
        }
        taskInstance = taskOptional.get();

        // Execute.
        boolean isNeedUpdate;
        try {
            fillDef(flowInstance);
            FlowContext.setCurrentFlow(flowInstance);
            isNeedUpdate = execute(taskInstance);
        } finally {
            FlowContext.removeCurrentFlow();
        }

        // Submit results.
        if (isNeedUpdate) {
            updateResult(taskInstance);
        }
    }

    public void execute(String flowId) {
        if (!flowLockFacade.acquireLock(flowId)) {
            return;
        }

        try {
            FlowInstance flowInstance = getFlow(flowId);
            if (flowInstance == null) {
                log.warn("Execute flowId: {}, " +
                        "but no such flow found.", flowId);
                return;
            }

            executePerfectlyUnsafe(flowInstance);
        } finally {
            flowLockFacade.releaseLock(flowId);
        }
    }

    public void execute(final FlowInstance flowInstance) {
        if (!flowLockFacade.acquireLock(flowInstance.getFlowId())) {
            return;
        }

        try {
            executePerfectlyUnsafe(flowInstance);
        } finally {
            flowLockFacade.releaseLock(flowInstance.getFlowId());
        }
    }

    private void executePerfectlyUnsafe(final FlowInstance flowInstance) {
        try {
            fillDef(flowInstance);
            FlowContext.setCurrentFlow(flowInstance);
            executeUnsafe(flowInstance);
            // Avoid multiple updates within 'executeUnsafe'.
            if (!flowInstance.getStatus().isTerminal()) {
                updateFlow(flowInstance);
            }
        } catch (Throwable throwable) {
            exceptionHandler(flowInstance, throwable);
            // After handling the exception, execute again.
            executeUnsafe(flowInstance);
        } finally {
            FlowContext.removeCurrentFlow();
        }
    }

    private boolean execute(final TaskInstance taskInstance) {

        boolean isNeedUpdate;

        try {
            isNeedUpdate = executeUnsafe(taskInstance);
        } catch (TerminateException throwable) {
            terminate(taskInstance.getFlowId(), throwable);
            isNeedUpdate = false;
        } catch (Throwable throwable) {
            taskInstance.setStatus(TaskStatus.FAILED);
            taskInstance.setReasonForNotCompleting("Execute exception: "
                    + ExceptionUtils.getMessage(throwable));
            isNeedUpdate = true;
        }

        return isNeedUpdate;
    }

    private void executeUnsafe(final FlowInstance flowInstance) {

        if (flowInstance.getStatus().isTerminal()) {
            terminal(flowInstance);
            return;
        }

        DecideResult decideResult = decide(flowInstance);
        if (decideResult.isComplete()) {
            updateTasks(decideResult.getTasksToBeUpdated());
            completeFlow(flowInstance);
            executeUnsafe(flowInstance);
            return;
        }

        List<TaskInstance> tasksToBeScheduled =
                decideResult.getTasksToBeScheduled();

        List<TaskInstance> tasksToBeUpdated =
                decideResult.getTasksToBeUpdated();

        List<TaskInstance> tasksToBeRetried =
                decideResult.getTasksToBeRetried();

        if (CollectionUtils.isNotEmpty(tasksToBeScheduled)) {

            scheduleTasks(flowInstance, tasksToBeScheduled)
                    .forEach(taskInstance -> {
                        if (executeUnsafe(taskInstance)) {
                            tasksToBeUpdated.add(taskInstance);
                        }
                    });
        }

        if (CollectionUtils.isNotEmpty(tasksToBeUpdated)) {
            updateTasks(tasksToBeUpdated);
        }

        if (CollectionUtils.isNotEmpty(tasksToBeRetried)) {
            addToQueue(tasksToBeRetried);
        }

        if (CollectionUtils.isNotEmpty(tasksToBeUpdated)) {
            executeUnsafe(flowInstance);
        }
    }

    private void terminate(String flowId, TerminateException throwable) {
        // Try to wait for a safe point for 30 seconds, otherwise forces terminate.
        boolean locked = flowLockFacade.acquireLock(flowId, LOCK_TRY_TIME_MS);
        if (!locked) {
            log.warn("Safe point not reached, force terminate flow: {}.", flowId);
        }

        try {
            Optional.ofNullable(getExecutionDAO().getFlow(flowId))
                    .ifPresent(flowInstance -> {
                                if (!flowInstance.getStatus().isTerminal()) {
                                    fillDef(flowInstance);
                                    terminateExceptionHandler(flowInstance, throwable);
                                    executePerfectlyUnsafe(flowInstance);
                                }
                            }
                    );
        } finally {
            if (locked) {
                flowLockFacade.releaseLock(flowId);
            }
        }
    }

    public void terminate(String flowId, String reason) {
        // Try to wait for a safe point for 30 seconds, otherwise forces terminate.
        boolean locked = flowLockFacade.acquireLock(flowId, LOCK_TRY_TIME_MS);
        if (!locked) {
            log.warn("Safe point not reached, force terminate flow: {}.", flowId);
        }

        try {
            Optional.ofNullable(getExecutionDAO().getFlow(flowId))
                    .ifPresent(flowInstance -> {
                                if (!flowInstance.getStatus().isTerminal()) {
                                    fillDef(flowInstance);
                                    flowInstance.setStatus(FlowStatus.TERMINATED);
                                    flowInstance.setReasonForNotCompleting(reason);
                                    Optional.ofNullable(flowInstance.getFlowDef())
                                            .ifPresent(flowDef ->
                                                    triggerFailureFlow(
                                                            flowInstance,
                                                            flowDef.getFailureFlowName())
                                            );
                                    executePerfectlyUnsafe(flowInstance);
                                }
                            }
                    );
        } finally {
            if (locked) {
                flowLockFacade.releaseLock(flowId);
            }
        }
    }

    public void pause(String flowId) {
        if (!flowLockFacade.acquireLock(flowId, LOCK_TRY_TIME_MS)) {
            throw new IllegalStateException(
                    String.format("Error acquiring lock when pause flow: %s", flowId));
        }

        try {
            Optional.ofNullable(getExecutionDAO().getFlow(flowId))
                    .ifPresent(flowInstance -> {
                                if (!flowInstance.getStatus().isTerminal()
                                        && !flowInstance.getStatus().isPaused()) {
                                    fillDef(flowInstance);
                                    flowInstance.setStatus(FlowStatus.PAUSED);
                                    Optional.ofNullable(flowInstance.getFlowDef())
                                            .ifPresent(flowDef ->
                                                    triggerFailureFlow(
                                                            flowInstance,
                                                            flowDef.getFailureFlowName())
                                            );
                                    executePerfectlyUnsafe(flowInstance);
                                }
                            }
                    );
        } finally {
            flowLockFacade.releaseLock(flowId);
        }
    }

    public void resume(String flowId) {
        if (!flowLockFacade.acquireLock(flowId, LOCK_TRY_TIME_MS)) {
            throw new IllegalStateException(
                    String.format("Error acquiring lock when resume flow: %s", flowId));
        }

        try {
            Optional.ofNullable(getExecutionDAO().getFlow(flowId))
                    .ifPresent(flowInstance -> {
                                if (flowInstance.getStatus().isPaused()) {
                                    fillDef(flowInstance);
                                    flowInstance.setStatus(FlowStatus.RUNNING);
                                    flowInstance.setLastUpdated(TimeUtils.currentTimeMillis());
                                    executePerfectlyUnsafe(flowInstance);
                                }
                            }
                    );
        } finally {
            flowLockFacade.releaseLock(flowId);
        }
    }

    public void retry(String flowId, boolean retrySubFlowIfPossibly) {
        FlowInstance flowInstance = getFlow(flowId);
        if (flowInstance == null ||
                CollectionUtils.isEmpty(
                        flowInstance.getTaskInstances())) {
            return;
        }

        if (!flowInstance.getStatus().isTerminal()) {
            throw new IllegalStateException(String.format(
                    "The flow instance %s is still running " +
                            "and therefore cannot be retied",
                    flowId));
        }

        FlowInstance firstFailedFlow =
                findLastFailedIfAny(flowInstance);

        if (firstFailedFlow == null) {
            return;
        }

        // If 'firstFailedFlow' is a sub-flow, retry the entire sub-flow:
        // logically, find the corresponding sub-flow task and try again.
        if (retrySubFlowIfPossibly
                && !StringUtils.equals(flowId, firstFailedFlow.getFlowId())
                && StringUtils.isNotBlank(firstFailedFlow.getParentFlowId())) {
            firstFailedFlow = getFlow(firstFailedFlow.getParentFlowId());
        }

        try {
            fillDef(firstFailedFlow);
            FlowContext.setCurrentFlow(firstFailedFlow);

            retry(firstFailedFlow);
        } finally {
            FlowContext.removeCurrentFlow();
        }
    }

    private void retry(final FlowInstance flowInstance) {

        List<String> hangingTaskIds =
                flowInstance.getTaskInstances()
                        .stream()
                        .filter(TaskInstance::isHanging)
                        .map(TaskInstance::getTaskId)
                        .collect(Collectors.toList());

        flowInstance.setTaskInstances(
                flowInstance.getTaskInstances()
                        .stream()
                        .filter(taskInstance ->
                                !taskInstance.isHanging())
                        .collect(Collectors.toList())
        );

        List<TaskInstance> failedTasks =
                flowInstance.getTaskInstances()
                        .stream()
                        .filter(taskInstance ->
                                taskInstance.getStatus()
                                        .isUnsuccessfullyTerminated())
                        .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(failedTasks)) {
            return;
        }

        flowInstance.setStatus(FlowStatus.RUNNING);
        flowInstance.setReasonForNotCompleting(null);

        // todo: trigger flowInstance decision

        updateFlow(flowInstance);

        // Delete related hang task before scheduling.
        hangingTaskIds.forEach(this::deleteTask);

        List<TaskInstance> tasksToBeScheduled =
                failedTasks.stream()
                        .map(taskInstance ->
                                taskToBeRescheduled(
                                        taskInstance, 0))
                        .collect(Collectors.toList());

        for (TaskInstance taskInstance : tasksToBeScheduled) {

            if (executeUnsafe(taskInstance)) {
                updateTask(taskInstance);
                execute(flowInstance.getFlowId());
            }
        }

        updateParents(flowInstance);
    }

    private TaskInstance taskToBeRescheduled(final TaskInstance toBeRetried,
                                             long startDelayMs) {
        TaskInstance retryTask = toBeRetried.copy();
        retryTask.setStartDelayMs(startDelayMs);
        retryTask.setRetryCount(
                toBeRetried.getRetryCount() + 1);
        retryTask.setStatus(TaskStatus.SCHEDULED);
        retryTask.setReasonForNotCompleting(null);
        retryTask.setOutput(null);
        retryTask.setSubFlowId(null);
        retryTask.setParentTaskId(null);
        retryTask.setHangTaskId(null);
        retryTask.setProgress(0);
        retryTask.setLink(null);
        retryTask.setRetryTime(
                TimeUtils.currentTimeMillis()
                        + retryTask.getStartDelayMs());
        retryTask.setScheduledTime(0);
        retryTask.setStartTime(0);
        retryTask.setLastUpdated(0);
        retryTask.setEndTime(0);

        retryTask.setExecuted(false);
        retryTask.setHanging(false);

        toBeRetried.setRetryCount(retryTask.getRetryCount());
        toBeRetried.setRetryTime(retryTask.getRetryTime());

        return retryTask;
    }

    private void updateParents(FlowInstance flowInstance) {
        while (flowInstance.hasParent()) {
            // Update parent's sub workflow task.
            TaskInstance subWorkflowTask =
                    getTask(flowInstance.getParentTaskId());

            subWorkflowTask.setStatus(TaskStatus.IN_PROGRESS);
            subWorkflowTask.setReasonForNotCompleting(null);

            updateTask(subWorkflowTask);

            FlowInstance parentFlow =
                    getFlow(flowInstance.getParentFlowId());

            parentFlow.setStatus(FlowStatus.RUNNING);
            parentFlow.setReasonForNotCompleting(null);

            updateFlow(parentFlow);

            // todo: trigger flowInstance decision

            // Reverse recursion.
            flowInstance = parentFlow;
        }
    }

    public void skipTask(final SkipTaskReq skipTaskReq) {
        if (!flowLockFacade.acquireLock(skipTaskReq.getFlowId(), LOCK_TRY_TIME_MS)) {
            throw new IllegalStateException(
                    String.format("Error acquiring lock when skip flow: %s task: %s",
                            skipTaskReq.getFlowId(),
                            skipTaskReq.getTaskName()));
        }

        try {
            skipTaskUnsafe(skipTaskReq);
        } finally {
            flowLockFacade.releaseLock(skipTaskReq.getFlowId());
        }
    }

    private void skipTaskUnsafe(final SkipTaskReq skipTaskReq) {
        FlowInstance flowInstance = getFlow(skipTaskReq.getFlowId());
        if (!flowInstance.getStatus().isRunning()) {
            throw new IllegalStateException(String.format(
                    "The flow instance %s is not running so the task %s cannot be skipped",
                    skipTaskReq.getFlowId(),
                    skipTaskReq.getTaskName()));
        }

        fillDef(flowInstance);

        TaskDef taskDef = getTaskDef(flowInstance, skipTaskReq.getTaskName());
        if (taskDef == null) {
            throw new IllegalStateException(String.format(
                    "The task %s does not exist in the flowDef %s",
                    skipTaskReq.getTaskName(),
                    flowInstance.getFlowName()));
        }

        /*
         There are three cases of skipping:
         Case 1: the task has not started yet.
         Case 2: the task has started but is not over yet.
         Case 3: the task has ended, return directly.
        */
        Optional<TaskInstance> optionalTaskInstance =
                flowInstance.getTaskByName(skipTaskReq.getTaskName());

        TaskInstance skippedTaskInstance = null;

        // Case 3.
        if (optionalTaskInstance.isPresent()
                && (skippedTaskInstance
                = optionalTaskInstance.get()).isExecuted()
                && !skippedTaskInstance.getStatus().isHanged()) {
            log.info("The task {} {} has ended, return directly",
                    skipTaskReq.getTaskName(),
                    skippedTaskInstance.getTaskId());
            return;
        }

        // Case 2.
        try {
            FlowContext.setCurrentFlow(flowInstance);

            if (optionalTaskInstance.isPresent()) {

                if (isCancellable(skippedTaskInstance)) {
                    T flowTask = flowTaskRegistry.getFlowTask(taskDef.getType());
                    try {
                        flowTask.cancel(skippedTaskInstance.deepCopy());
                    } catch (Throwable throwable) {
                        log.error(
                                "Error canceling flow task:{}/{} in flow: {}",
                                taskDef.getType(),
                                skippedTaskInstance.getTaskId(),
                                flowInstance.getFlowId(),
                                throwable);
                    }
                }

                final List<TaskInstance> tasksToBeUpdated = new LinkedList<>();
                tasksToBeUpdated.add(skippedTaskInstance);

                // Skip hanging tasks.
                if (skippedTaskInstance.getStatus().isHanged()) {
                    flowInstance.getTaskById(skippedTaskInstance.getHangTaskId())
                            .filter(TaskInstance::isHanging)
                            .filter(__ -> !__.isExecuted())
                            .ifPresent(hanging -> {
                                hanging.setStatus(TaskStatus.SKIPPED);
                                hanging.setEndTime(TimeUtils.currentTimeMillis());
                                hanging.setReasonForNotCompleting("Active Skip");

                                tasksToBeUpdated.add(hanging);
                            });
                }

                skippedTaskInstance.setExecuted(false);
                skippedTaskInstance.setStatus(TaskStatus.SKIPPED);
                skippedTaskInstance.setOutput(skipTaskReq.getOutput());
                skippedTaskInstance.setEndTime(TimeUtils.currentTimeMillis());

                updateTasks(tasksToBeUpdated);
            }
        } finally {
            FlowContext.removeCurrentFlow();
        }

        // Case 1.
        flowInstance.getSkipTasks().add(skipTaskReq.getTaskName());

        // Decide.
        executePerfectlyUnsafe(flowInstance);
    }

    public FlowInstance getCropFlow(String flowId) {
        return crop(getFlow(flowId));
    }

    public FlowInstance getCropFlowByCorrelationId(String correlationId) {
        return crop(getFlowByCorrelationId(correlationId));
    }

    public FlowInstance getFlow(String flowId) {
        return getExecutionDAO().getFlow(flowId);
    }

    public FlowInstance getFlowByCorrelationId(String correlationId) {
        return getExecutionDAO().getFlowByCorrelationId(correlationId);
    }

    private FlowInstance crop(final FlowInstance flowInstance) {
        if (flowInstance == null) {
            return null;
        }
        flowInstance.setTaskInstances(
                flowInstance.getTaskInstances()
                        .stream()
                        .filter(__ -> !__.isHanging())
                        .collect(Collectors.toList())
        );
        return flowInstance;
    }

    public TaskInstance getTask(String taskId) {
        return getExecutionDAO().getTask(taskId);
    }

    public List<String> getRunningTaskIds(String flowId) {
        FlowInstance flowInstance = getFlow(flowId);
        if (flowInstance != null) {
            return flowInstance.getTaskInstances().stream()
                    .filter(taskInstance ->
                            TaskStatus.IN_PROGRESS.equals(
                                    taskInstance.getStatus()))
                    .map(TaskInstance::getTaskId)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private DecideResult decide(final FlowInstance flowInstance) {

        DecideResult result = new DecideResult();

        if (flowInstance.getStatus().isTerminal()) {
            return result;
        }

        checkFlowTimeout(flowInstance);

        if (flowInstance.getStatus().isPaused()) {
            return result;
        }

        Set<String> executedTaskNames = new HashSet<>();

        List<TaskInstance> pendingTasks = new ArrayList<>();

        flowInstance.getTaskInstances().forEach(taskInstance -> {
            if (taskInstance.isExecuted()) {
                executedTaskNames.add(taskInstance.getTaskName());
            } else {
                pendingTasks.add(taskInstance);
            }
        });

        // Are there any tasks that are being retried?
        boolean hasRetriesInProgress = false;

        final Map<String, TaskInstance> tasksToBeScheduled = new LinkedHashMap<>();

        if (CollectionUtils.isEmpty(pendingTasks)
                && CollectionUtils.isEmpty(executedTaskNames)) {

            if (CollectionUtils.isEmpty(
                    flowInstance.getFlowDef().getTaskDefs())) {
                throw new TerminateException(
                        "No tasks found to be executed",
                        FlowStatus.COMPLETED);
            }

            TaskDef toBeScheduled = flowInstance.getFlowDef().getTaskDefs().get(0);

            while (isTaskSkipped(flowInstance, toBeScheduled)) {
                toBeScheduled = getNextTask(flowInstance, toBeScheduled);
            }

            if (toBeScheduled != null) {
                getMappedTasks(flowInstance, toBeScheduled)
                        .forEach(taskInstance ->
                                tasksToBeScheduled.put(
                                        taskInstance.getTaskName(), taskInstance)
                        );
            }

        } else {

            for (TaskInstance pendingTask : pendingTasks) {

                if (!pendingTask.getStatus().isTerminal()) {

                    checkTaskTimeout(pendingTask);

                    if (pendingTask.getStatus().isScheduled()
                            && pendingTask.getRetryTime()
                            < TimeUtils.currentTimeMillis()) {

                        tasksToBeScheduled.putIfAbsent(
                                pendingTask.getTaskName(), pendingTask);

                        executedTaskNames.remove(pendingTask.getTaskName());
                    }
                }

                if (!pendingTask.getStatus().isSuccessful()) {

                    TaskInstance retryTask = getRetryTask(pendingTask);
                    if (retryTask != null) {
                        result.getTasksToBeRetried().add(retryTask);
                        result.getTasksToBeUpdated().add(pendingTask);
                    }

                    hasRetriesInProgress = true;

                } else if (!pendingTask.isExecuted()
                        && pendingTask.getStatus().isTerminal()) {

                    pendingTask.setExecuted(true);

                    // Process hang logic.
                    if (pendingTask.getStatus().isHanged()) {
                        TaskDef.HangDef hangDef = pendingTask.getTaskDef().getHangDef();
                        getMappedTasks(flowInstance, pendingTask, hangDef.getDetermineTaskDef())
                                .forEach(taskInstance -> {
                                            if (tasksToBeScheduled.putIfAbsent(
                                                    taskInstance.getTaskName(), taskInstance) == null) {
                                                taskInstance.setHanging(true);
                                                taskInstance.setParentTaskId(pendingTask.getTaskId());
                                                pendingTask.setHangTaskId(taskInstance.getTaskId());
                                            }
                                        }
                                );
                    } else if (pendingTask.isHanging()) {

                        Optional.ofNullable(feedbackHang(flowInstance, pendingTask))
                                .filter(result.getTasksToBeUpdated()::add);

                    } else {
                        Optional.ofNullable(getNextTask(flowInstance, pendingTask.getTaskDef()))
                                .ifPresent(nextTaskDef ->
                                        getMappedTasks(flowInstance, nextTaskDef).forEach(
                                                taskInstance -> tasksToBeScheduled.putIfAbsent(
                                                        taskInstance.getTaskName(), taskInstance
                                                )
                                        )
                                );
                    }

                    result.getTasksToBeUpdated().add(pendingTask);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(executedTaskNames)) {
            result.getTasksToBeScheduled().addAll(
                    tasksToBeScheduled.values()
                            .stream()
                            .filter(task -> !executedTaskNames.contains(task.getTaskName()))
                            .collect(Collectors.toList())
            );
        } else {
            result.getTasksToBeScheduled().addAll(tasksToBeScheduled.values());
        }

        if (!hasRetriesInProgress
                && CollectionUtils.isEmpty(result.getTasksToBeScheduled())
                && checkForFlowCompletion(flowInstance)) {
            result.setComplete(true);
        }

        return result;
    }

    public List<TaskInstance> getMappedTasks(final FlowInstance flowInstance,
                                             final TaskDef taskDef) {

        return getMappedTasks(flowInstance, null, taskDef);
    }

    @SuppressWarnings("unchecked")
    public List<TaskInstance> getMappedTasks(final FlowInstance flowInstance,
                                             final TaskInstance parentTask,
                                             final TaskDef taskDef) {

        final FlowTask flowTask = flowTaskRegistry.getFlowTask(taskDef.getType());

        int verifyPhase = 0;
        Object taskInput;

        try {
            // External parameters, refer to the input of external JSON and flow task configuration.
            verifyPhase = 1;
            taskInput = Optional.ofNullable(parentTask)
                    .map(parent -> getTaskInput(parent, taskDef))
                    .orElseGet(() -> getTaskInput(flowInstance, taskDef));

            if (taskInput instanceof Map) {
                verifyPhase = 2;
                flowTask.verify(new Configuration((Map<String, Object>) taskInput));
            }

            // Convert external parameters to internal parameters,
            // the type of internal parameter, specified by #{task}.
            verifyPhase = 3;
            taskInput = flowTask.getInput(taskInput);
        } catch (ValidationException | IllegalArgumentException e) {
            // The parameter is wrong, the flow needs to be terminated immediately.
            throw new TerminateException(String.format(
                    "Illegal task(%s) input, verify stage:(%d) illegal details:(%s)",
                    taskDef.getName(), verifyPhase, e.getLocalizedMessage()));
        }

        return flowTask.getMappedTasks(TaskMapperContext
                .builder()
                .flowInstance(flowInstance)
                .taskDef(taskDef)
                .input(taskInput)
                .build());
    }

    private void terminateUnsafe(final FlowInstance flowInstance,
                                 final FlowStatus status,
                                 String reason,
                                 String failureFlowName) {

        if (flowInstance.getStatus().isTerminal()) {
            return;
        }

        String flowId = flowInstance.getFlowId();

        flowInstance.setStatus(
                Optional.ofNullable(status)
                        .orElse(FlowStatus.TERMINATED));

        flowInstance.setReasonForNotCompleting(reason);

        try {
            triggerFailureFlow(flowInstance, failureFlowName);
        } catch (Throwable throwable) {
            log.error("Failed to start failure flow: {}",
                    failureFlowName, throwable);
        }

        List<String> erroredTasks = cancelNonTerminalTasks(flowInstance);

        if (CollectionUtils.isNotEmpty(erroredTasks)) {
            throw new FlowException(FLOW_EXECUTION_ERROR,
                    String.format("Error canceling flow: %s tasks: %s",
                            flowId, String.join(Delimiter.COMMA, erroredTasks)));
        }
    }

    private void triggerFailureFlow(final FlowInstance flowInstance, String failureFlowName) {
        if (StringUtils.isNotBlank(failureFlowName)) {
            Optional.ofNullable(metadataService.getFlow(failureFlowName))
                    .ifPresent(flowDef -> {
                        Object failureFlowInput =
                                getMappingValue(flowInstance, flowDef.getInput());

                        FlowDef replacedFlowDef = flowDef.copy();
                        replacedFlowDef.setInput(null);

                        String failureFlowId = startFlow(StartFlowReq.builder()
                                .name(failureFlowName)
                                .flowDef(replacedFlowDef)
                                .input(failureFlowInput)
                                .build()
                        );

                        flowInstance.setFailureFlowId(failureFlowId);
                    });
        }
    }

    private List<String> cancelNonTerminalTasks(final FlowInstance flowInstance) {
        final List<String> errorTasks = new LinkedList<>();

        if (CollectionUtils.isNotEmpty(flowInstance.getTaskInstances())) {
            for (TaskInstance taskInstance : flowInstance.getTaskInstances()) {
                if (isCancellable(taskInstance)) {

                    final TaskDef taskDef = taskInstance.getTaskDef();

                    T flowTask = flowTaskRegistry.getFlowTask(taskDef.getType());

                    try {
                        if (!taskInstance.getStatus().isTerminal()
                                || taskInstance.isRetryable()) {
                            taskInstance.setStatus(TaskStatus.CANCELED);
                        }

                        flowTask.cancel(taskInstance);

                        updateTask(taskInstance);
                    } catch (Throwable throwable) {
                        taskInstance.setReasonForNotCompleting(
                                throwable.getLocalizedMessage());
                        errorTasks.add(taskInstance.getTaskDef().getName());
                        log.error(
                                "Error canceling flow task:{}/{} in flow: {}",
                                taskDef.getType(),
                                taskInstance.getTaskId(),
                                flowInstance.getFlowId(),
                                throwable);
                    }
                }
            }
        }

        return errorTasks;
    }

    private boolean isCancellable(final TaskInstance taskInstance) {
        return !taskInstance.getStatus().isTerminal()
                // If the task is executed asynchronously, it also needs to be canceled.
                || taskInstance.getStatus().isHanged()
                // Task is retrying.
                || taskInstance.isRetryable();
    }

    private void checkFlowTimeout(final FlowInstance flowInstance) {
        FlowDef flowDef = flowInstance.getFlowDef();
        if (flowDef == null) {
            log.warn("Missing flow definition : {}", flowInstance.getFlowId());
            return;
        }

        if (flowInstance.getStatus().isTerminal()) {
            return;
        }

        FlowDef.ControlDef controlDef = flowDef.getControlDef();
        if (controlDef == null) {
            return;
        }

        if (controlDef.getTimeoutMs() <= 0) {
            return;
        }

        long elapsedTime = TimeUtils.currentTimeMillis() - flowInstance.getStartTime();

        if (elapsedTime < controlDef.getTimeoutMs()) {
            return;
        }

        String reason = String.format(
                "Flow timed out after %d milliseconds. Timeout configured as %d milliseconds. " +
                        "Timeout policy configured to %s",
                elapsedTime,
                controlDef.getTimeoutMs(),
                controlDef.getTimeoutPolicy().name());

        switch (controlDef.getTimeoutPolicy()) {
            case ALERT_ONLY:
                log.warn(reason);
                return;
            case TIME_OUT:
                throw new TerminateException(reason, FlowStatus.TIMED_OUT);
        }
    }

    private void checkTaskTimeout(final TaskInstance taskInstance) {
        final TaskDef taskDef = taskInstance.getTaskDef();

        if (taskDef == null) {
            log.warn(
                    "Missing task definition for task:{} in flow:{}",
                    taskInstance.getTaskId(),
                    taskInstance.getFlowId());
            return;
        }

        TaskDef.ControlDef controlDef = taskDef.getControlDef();
        if (controlDef == null) {
            return;
        }

        if (taskInstance.getStatus().isTerminal()
                || controlDef.getTimeoutMs() <= 0
                || taskInstance.getStartTime() <= 0) {
            return;
        }

        long timeout = controlDef.getTimeoutMs();
        long now = TimeUtils.currentTimeMillis();
        long elapsedTime = now - (taskInstance.getStartTime() + controlDef.getStartDelayMs());

        if (elapsedTime < timeout) {
            return;
        }

        String reason = String.format(
                "Task timed out after %d milliseconds. Timeout configured as %d milliseconds. "
                        + "Timeout policy configured to %s",
                elapsedTime,
                controlDef.getTimeoutMs(),
                controlDef.getTimeoutPolicy().name());

        switch (controlDef.getTimeoutPolicy()) {
            case ALERT_ONLY:
                log.warn(reason);
                return;
            case RETRY:
                taskInstance.setStatus(TaskStatus.TIMED_OUT);
                taskInstance.setReasonForNotCompleting(reason);
                return;
            case TIME_OUT:
                taskInstance.setStatus(TaskStatus.TIMED_OUT);
                taskInstance.setReasonForNotCompleting(reason);
                throw new TerminateException(reason, FlowStatus.TIMED_OUT, taskInstance);
        }
    }

    public TaskDef getTaskDef(final FlowInstance flowInstance, String targetName) {
        final FlowDef flowDef = flowInstance.getFlowDef();
        if (flowDef == null || flowDef.getTaskDefs() == null) {
            return null;
        }
        for (TaskDef taskDef : flowDef.getTaskDefs()) {
            if (targetName.equals(taskDef.getName())) {
                return taskDef;
            } else if (taskDef.getHangDef() != null) {
                TaskDef determineTaskDef =
                        taskDef.getHangDef()
                                .getDetermineTaskDef();
                if (targetName.equals(determineTaskDef.getName())) {
                    return determineTaskDef;
                }
            }
        }
        return null;
    }

    public TaskDef getNextTask(final FlowInstance flowInstance, final TaskDef target) {
        final FlowDef flowDef = flowInstance.getFlowDef();
        if (flowDef == null || flowDef.getTaskDefs() == null) {
            return null;
        }

        final Iterator<TaskDef> iterator = flowDef.getTaskDefs().iterator();

        TaskDef toBeScheduled = getNextTask(iterator, target);

        while (isTaskSkipped(flowInstance, toBeScheduled)) {
            toBeScheduled = getNextTask(iterator, toBeScheduled);
        }

        return toBeScheduled;
    }

    private TaskDef getNextTask(final Iterator<TaskDef> iterator, final TaskDef target) {

        while (iterator.hasNext()) {
            TaskDef nextTask = getNextTask(iterator.next(), target);
            if (nextTask == TaskDef.MATCHED) {
                break;
            } else if (nextTask != null) {
                return nextTask;
            }
        }

        return iterator.hasNext() ? iterator.next() : null;
    }

    public TaskDef getNextTask(final TaskDef toBeSearched, final TaskDef target) {
        return flowTaskRegistry.getFlowTask(toBeSearched.getType())
                .next(toBeSearched, target);
    }

    private boolean isTaskSkipped(final FlowInstance flowInstance, final TaskDef toBeScheduled) {
        try {
            boolean isTaskSkipped = false;

            if (toBeScheduled != null) {
                isTaskSkipped = flowInstance.getSkipTasks()
                        .contains(toBeScheduled.getName());
                TaskDef.ControlDef controlDef = toBeScheduled.getControlDef();
                TaskDef.SkipDef skipDef;

                if (!isTaskSkipped
                        && controlDef != null
                        && (skipDef = controlDef.getSkipDef()) != null
                        && StringUtils.isNotBlank(skipDef.getSkipCondition())) {

                    isTaskSkipped =
                            BooleanUtils.toBoolean(
                                    String.valueOf(
                                            engineActuator.compute(
                                                    skipDef.getEngineType(),
                                                    skipDef.getSkipCondition(),
                                                    flowContext(flowInstance))
                                    )
                            );

                    if (isTaskSkipped) {
                        flowInstance.getSkipTasks().add(toBeScheduled.getName());
                    }
                }
            }

            return isTaskSkipped;
        } catch (Throwable throwable) {
            throw new TerminateException(throwable);
        }
    }

    private TaskInstance getRetryTask(final TaskInstance toBeRetried) {
        // In the process of retrying the task.
        if (TimeUtils.currentTimeMillis() < toBeRetried.getRetryTime()) {
            return null;
        }

        int expectedRetryCount = 0;

        TaskDef.ControlDef controlDef =
                toBeRetried.getTaskDef().getControlDef();
        if (controlDef != null) {
            expectedRetryCount = controlDef.getRetryCount();
        }

        int retryCount = toBeRetried.getRetryCount();

        if (!toBeRetried.getStatus().isRetryable()
                || expectedRetryCount <= retryCount) {
            FlowStatus flowStatus;
            String reason = toBeRetried.getReasonForNotCompleting();
            switch (toBeRetried.getStatus()) {
                case CANCELED:
                    flowStatus = FlowStatus.TERMINATED;
                    break;
                case TIMED_OUT:
                    flowStatus = FlowStatus.TIMED_OUT;
                    break;
                case RETRIED:
                    // Exhausting the number of retries is also considered a timeout.
                    reason = String.format(
                            "The number of retries %d has been exhausted", retryCount);
                    flowStatus = FlowStatus.TIMED_OUT;
                    toBeRetried.setStatus(TaskStatus.TIMED_OUT);
                    break;
                default:
                    flowStatus = FlowStatus.FAILED;
                    break;
            }
            throw new TerminateException(reason, flowStatus, toBeRetried);
        }

        // Impossible.
        assert controlDef != null;

        long startDelayMs = 0;

        switch (controlDef.getRetryLogic()) {
            case FIXED:
                startDelayMs = controlDef.getRetryDelayMs();
                break;
            case EXPONENTIAL_BACKOFF:
                startDelayMs = controlDef.getRetryDelayMs() *
                        (long) Math.pow(2, toBeRetried.getRetryCount());
                startDelayMs = startDelayMs < 0 ? Integer.MAX_VALUE : startDelayMs;
                break;
        }

        // Build retry task.
        return taskToBeRescheduled(toBeRetried, startDelayMs);
    }

    private static String deduplicateKey(TaskInstance taskInstance) {
        return taskInstance.getTaskDef().getName();
    }

    private List<TaskInstance> scheduleTasks(final FlowInstance flowInstance,
                                             final List<TaskInstance> taskInstances) {

        List<TaskInstance> tasksToBeQueued = new ArrayList<>();
        List<TaskInstance> deduplicatedTasks =
                deduplicateTasks(flowInstance, taskInstances);

        if (CollectionUtils.isEmpty(deduplicatedTasks)) {
            return taskInstances
                    .stream()
                    .filter(__ -> !needToQueue(__))
                    .collect(Collectors.toList());
        }

        try {
            List<TaskInstance> createdTasks = createTasks(deduplicatedTasks);

            if (CollectionUtils.isNotEmpty(createdTasks)) {

                createdTasks.forEach(createdTask -> {
                    if (needToQueue(createdTask)) {
                        tasksToBeQueued.add(createdTask);
                    }
                });

                flowInstance.getTaskInstances().addAll(createdTasks);
            }

        } catch (Throwable throwable) {
            String errorMsg = String.format(
                    "Error scheduling tasks: %s, for flow: %s",
                    taskInstances.stream()
                            .map(TaskInstance::getTaskId)
                            .collect(Collectors.toList()),
                    flowInstance.getFlowId());
            log.error(errorMsg, throwable);
            throw new TerminateException(errorMsg);
        }

        if (CollectionUtils.isNotEmpty(tasksToBeQueued)) {
            addToQueue(tasksToBeQueued);
            taskInstances.removeAll(tasksToBeQueued);
        }

        return taskInstances;
    }

    private List<TaskInstance> deduplicateTasks(final FlowInstance flowInstance,
                                                final List<TaskInstance> taskInstances) {

        if (CollectionUtils.isEmpty(taskInstances)) {
            return Collections.emptyList();
        }

        List<String> tasksInFlow = flowInstance.getTaskInstances().stream()
                .map(FlowExecutor::deduplicateKey)
                .collect(Collectors.toList());

        List<TaskInstance> deduplicatedTasks = taskInstances;

        if (CollectionUtils.isNotEmpty(tasksInFlow)) {
            deduplicatedTasks = taskInstances.stream()
                    .filter(taskInstance ->
                            !tasksInFlow.contains(
                                    deduplicateKey(taskInstance)))
                    .collect(Collectors.toList());
        }

        return deduplicatedTasks;
    }

    private boolean needToQueue(final TaskInstance taskInstance) {
        // delayed scheduling
        return taskInstance.getStartDelayMs() > 0;
    }

    private void addToQueue(final List<TaskInstance> taskInstances) {
        getQueueService().offer(getTaskDelayQueueName(),
                taskInstances.stream().map(taskInstance -> {
                    QueueMessage message = new QueueMessage();
                    message.setType(taskInstance.getTaskName());
                    message.setId(Joiner.on(Delimiter.AT).join(
                            taskInstance.getTaskId(),
                            taskInstance.getRetryCount()
                    ));
                    message.setDelayMs(taskInstance.getStartDelayMs());
                    return message;
                }).collect(Collectors.toList())
        );
    }

    private boolean executeUnsafe(final TaskInstance taskInstance) {
        if (taskInstance.getStatus().isFinished()) {
            log.warn("Task instance:{} finished", taskInstance.getTaskId());
            return false;
        }

        // The scheduling time has not yet arrived.
        if (TimeUtils.currentTimeMillis()
                < taskInstance.getScheduledTime()) {
            return false;
        }

        // The scheduling status first checks the concurrency degree,
        // and it can be executed only after it is satisfied.
        // Otherwise, execute the retry logic.
        if (taskInstance.getStatus().isScheduled()
                && checkConcurrency(taskInstance)) {
            TaskInstance retryTask = getRetryTask(taskInstance);
            if (retryTask != null) {
                taskInstance.setReasonForNotCompleting(
                        taskInstance.getReasonForNotCompleting()
                                + "(concurrency limit)"
                );
                addToQueue(Collections.singletonList(retryTask));
                return true;
            } else {
                return false;
            }
        }

        log.info("Start execute task: {} {}",
                taskInstance.getTaskName(), taskInstance.getTaskId());

        try {
            FlowContext.setCurrentTask(taskInstance);

            boolean firstSchedule =
                    taskInstance.getStatus().isScheduled();

            taskInstance.setStatus(TaskStatus.IN_PROGRESS);

            if (firstSchedule) {
                taskAspect.onCreated(taskInstance);
            }

            if (0 == taskInstance.getStartTime()) {
                taskInstance.setStartTime(TimeUtils.currentTimeMillis());
            }

            // 'COMPLETED' output supports caching.
            boolean enableCache = isEnableCache(taskInstance);
            String cacheKey = null;
            FlowCache flowCache = null;
            if (enableCache) {
                cacheKey = getCacheKey(taskInstance);
                flowCache = flowCacheFactory.getDefaultCache();
                Object output = flowCache.get(cacheKey);
                if (output != null) {
                    taskInstance.setOutput(output);
                    taskInstance.setStatus(TaskStatus.COMPLETED);
                    return true;
                }
            }

            boolean result = flowTaskRegistry
                    .getFlowTask(taskInstance.getTaskDef().getType())
                    .execute(taskInstance);

            if (result) {
                buildLink(taskInstance);

                taskInstance.setOutput(getTaskOutput(taskInstance));

                if (taskInstance.getStatus().isCompleted()) {
                    if (enableCache) {
                        flowCache.put(cacheKey, taskInstance.getOutput());
                    } else {
                        checkAndHang(taskInstance);
                    }
                }
            }
            return firstSchedule || result;
        } catch (TerminateException terminateException) {
            terminateException.setTaskInstance(taskInstance);
            throw terminateException;
        } finally {
            taskInstance.setEndTime(TimeUtils.currentTimeMillis());
            log.info("Finish execute task: {} {}",
                    taskInstance.getTaskName(), taskInstance.getTaskId());
            FlowContext.removeCurrentTask();
        }
    }

    private boolean isEnableCache(final TaskInstance taskInstance) {
        TaskDef.ControlDef controlDef = taskInstance.getTaskDef().getControlDef();
        if (controlDef == null ||
                controlDef.getEnableCache() == null) {
            return false;
        }
        return controlDef.getEnableCache();
    }

    private String getCacheKey(final TaskInstance taskInstance) {
        // Note: JSON order problem
        return taskInstance.getTaskName()
                + Delimiter.COLON
                + JsonUtils.toJsonString(taskInstance.getInput());
    }

    private void buildLink(final TaskInstance taskInstance) {
        TaskDef.LinkDef linkDef = taskInstance.getTaskDef().getLinkDef();
        if (linkDef == null ||
                taskInstance.getLink() != null) {
            return;
        }

        try {
            TaskInstance.Link link = new TaskInstance.Link();
            link.setTitle(linkDef.getTitle());
            Optional.ofNullable(getMappingValue(
                            FlowContext.getCurrentFlow(), linkDef.getUrl()))
                    .ifPresent(url ->
                            link.setUrl(String.valueOf(url)));
            taskInstance.setLink(link);
        } catch (Throwable ignored) {
        }
    }

    private void checkAndHang(final TaskInstance taskInstance) {
        if (taskInstance.getTaskDef().getCheckDef() != null) {
            if (!checkSuccess(taskInstance)) {
                log.info("Business failure, reason: {}",
                        taskInstance.getReasonForNotCompleting());

                taskInstance.setStatus(TaskStatus.FAILED_WITH_TERMINAL_ERROR);
                return;
            } else if (checkRetry(taskInstance)) {
                log.info("The retry condition is met, waiting for retry, " +
                                "current {} retries, taskName: {} taskId: {}",
                        taskInstance.getRetryCount(),
                        taskInstance.getTaskName(),
                        taskInstance.getTaskId());

                taskInstance.setStatus(TaskStatus.RETRIED);
                taskInstance.setReasonForNotCompleting("Waiting for retry");
                return;
            }
        }

        if (taskInstance.getTaskDef().getHangDef() != null) {
            processHang(taskInstance);
        }
    }

    private boolean checkSuccess(final TaskInstance taskInstance) {
        TaskDef.CheckDef checkDef =
                taskInstance.getTaskDef().getCheckDef();
        TaskDef.SuccessDef successDef = checkDef.getSuccessDef();
        if (successDef == null) {
            return true;
        }

        if (StringUtils.isNotBlank(successDef.getSuccessCondition())) {
            boolean isSuccess = BooleanUtils.toBoolean(
                    String.valueOf(
                            engineActuator.compute(
                                    checkDef.getEngineType(),
                                    successDef.getSuccessCondition(),
                                    taskInstance.getOutput())
                    )
            );

            if (!isSuccess && StringUtils.isNotBlank(successDef.getFailureReasonExpression())) {
                taskInstance.setReasonForNotCompleting(JsonUtils.toJsonString(
                                engineActuator.compute(
                                        checkDef.getEngineType(),
                                        successDef.getFailureReasonExpression(),
                                        taskInstance.getOutput())
                        )
                );
            }

            return isSuccess;
        }

        return true;
    }

    private boolean checkRetry(final TaskInstance taskInstance) {
        TaskDef.CheckDef checkDef =
                taskInstance.getTaskDef().getCheckDef();
        TaskDef.RetryDef retryDef = checkDef.getRetryDef();
        if (retryDef == null) {
            return false;
        }

        if (StringUtils.isNotBlank(retryDef.getRetryCondition())) {
            return BooleanUtils.toBoolean(
                    String.valueOf(
                            engineActuator.compute(
                                    checkDef.getEngineType(),
                                    retryDef.getRetryCondition(),
                                    taskInstance.getOutput())
                    )
            );
        }
        return false;
    }

    private void processHang(final TaskInstance taskInstance) {
        TaskDef.HangDef hangDef =
                taskInstance.getTaskDef().getHangDef();
        if (hangDef == null || hangDef.getDetermineTaskDef() == null) {
            return;
        }

        taskInstance.setStatus(TaskStatus.HANGED);
    }

    private TaskInstance feedbackHang(final FlowInstance flowInstance,
                                      final TaskInstance hangingTask) {

        return Optional.ofNullable(hangingTask.getParentTaskId())
                .filter(StringUtils::isNotBlank)
                .flatMap(flowInstance::getTaskById)
                .filter(parentTask ->
                        parentTask.getStatus().isHanged())
                .map(parentTask -> {

                    if (hangingTask.getStatus().isCompleted()) {
                        parentTask.setStatus(TaskStatus.COMPLETED);
                    } else {
                        parentTask.setStatus(TaskStatus.FAILED_WITH_TERMINAL_ERROR);
                        parentTask.setReasonForNotCompleting(Optional.ofNullable(
                                hangingTask.getReasonForNotCompleting()).orElse("Hanged"));
                    }

                    if (parentTask.getTaskDef().getHangDef().isFeedbackOutput()) {
                        parentTask.setOutput(hangingTask.getOutput());
                    }

                    parentTask.setExecuted(false);
                    parentTask.setEndTime(TimeUtils.currentTimeMillis());

                    return parentTask;
                })
                .orElse(null);
    }

    private void exceptionHandler(final FlowInstance flowInstance, final Throwable throwable) {
        if (throwable instanceof TerminateException) {
            terminateExceptionHandler(flowInstance, (TerminateException) throwable);
        } else if (throwable instanceof IllegalArgumentException) {
            terminateExceptionHandler(flowInstance, new TerminateException(throwable));
        }
        // Retryable exception.
        else if (throwable instanceof BizException) {
            flowInstance.setStatus(FlowStatus.FAILED);
            flowInstance.setReasonForNotCompleting(throwable.getMessage());
        } else {
            terminateUnsafe(flowInstance,
                    FlowStatus.FAILED,
                    ExceptionUtils.getMessage(throwable),
                    flowInstance.getFlowDef().getFailureFlowName());
        }
    }

    private void terminateExceptionHandler(final FlowInstance flowInstance,
                                           final TerminateException throwable) {
        Optional.ofNullable(throwable.getTaskInstance())
                .ifPresent(throwableTask -> {

                    List<TaskInstance> tasksToBeUpdated
                            = new LinkedList<>();

                    if (throwableTask.isHanging()) {
                        Optional.ofNullable(feedbackHang(
                                        flowInstance, throwableTask))
                                .filter(tasksToBeUpdated::add);
                    }

                    tasksToBeUpdated.add(throwableTask);

                    updateTasks(tasksToBeUpdated);
                });

        terminateUnsafe(flowInstance,
                throwable.getFlowStatus(),
                throwable.getLocalizedMessage(),
                flowInstance.getFlowDef().getFailureFlowName());
    }

    private void terminal(final FlowInstance flowInstance) {
        // Confirm again whether the termination condition is met.
        if (!flowInstance.getStatus().isTerminal()) {
            return;
        }

        if (!flowInstance.getStatus().isSuccessful()) {
            cancelNonTerminalTasks(flowInstance);
        }

        try {
            updateOutput(flowInstance);
            flowAspect.onTerminated(flowInstance);
            updateParentFlow(flowInstance);
            updateFlow(flowInstance);
            Optional.ofNullable(
                            requestCallbacks.get(
                                    flowInstance.getFlowId()))
                    .ifPresent(RequestCallback::callback);
        } finally {
            flowLockFacade.deleteLock(flowInstance.getFlowId());
        }
    }

    private void updateOutput(final FlowInstance flowInstance) {
        flowInstance.setOutput(getFlowOutput(flowInstance));
    }

    private void completeFlow(final FlowInstance flowInstance) {

        if (flowInstance.getStatus().equals(FlowStatus.COMPLETED)) {
            return;
        }

        if (flowInstance.getStatus().isTerminal()) {
            String errorMsg = "Flow is already in terminal state. Current status: "
                    + flowInstance.getStatus();
            throw new FlowException(FLOW_EXECUTION_CONFLICT, errorMsg);
        }

        flowInstance.setStatus(FlowStatus.COMPLETED);
    }

    public void createFlow(final FlowInstance flowInstance) {
        flowAspect.onCreating(flowInstance);
        getExecutionDAO(flowInstance).createFlow(flowInstance);
        flowAspect.onCreated(flowInstance);
        log.info("Create new flow instance: {} {}",
                flowInstance.getFlowName(), flowInstance.getFlowId());
    }

    private void updateFlow(final FlowInstance flowInstance) {
        flowInstance.setLastUpdated(TimeUtils.currentTimeMillis());
        if (flowInstance.getStatus().isTerminal()) {
            flowInstance.setEndTime(TimeUtils.currentTimeMillis());
        }
        getExecutionDAO().updateFlow(flowInstance);
    }

    private void updateParentFlow(final FlowInstance flowInstance) {
        if (StringUtils.isBlank(flowInstance.getParentFlowId())
                || StringUtils.isBlank(flowInstance.getParentTaskId())) {
            return;
        }

        TaskResult taskResult = new TaskResult(
                flowInstance.getParentFlowId(),
                flowInstance.getParentTaskId(),
                convertStatus(flowInstance.getStatus())
        );
        taskResult.setOutput(flowInstance.getOutput());
        taskResult.setReasonForNotCompleting(
                flowInstance.getReasonForNotCompleting());

        // Consider this ring scenario: subFlowTask -> subFlow -> subFlowTask.
        // Therefore, to avoid chaotic update scenarios,
        // we use asynchronous to break the ring.
        asyncExecutor.execute(() -> {
            try {
                updateTask(taskResult);
            } catch (Throwable throwable) {
                // Todo: design an asynchronous retry mechanism.
                log.warn("When updateTask parentFlowId: {} parentTaskId: {}\n" +
                                "occurred throwable: {}",
                        flowInstance.getParentFlowId(),
                        flowInstance.getParentTaskId(),
                        ExceptionUtils.getMessage(throwable));
            }
        });
    }

    private TaskStatus convertStatus(FlowStatus flowStatus) {
        switch (flowStatus) {
            case COMPLETED:
                return TaskStatus.COMPLETED;
            case FAILED:
            case TERMINATED:
            case PAUSED:
                return TaskStatus.FAILED;
            case TIMED_OUT:
                return TaskStatus.TIMED_OUT;
            case RUNNING:
                return TaskStatus.IN_PROGRESS;
        }
        return TaskStatus.FAILED;
    }

    public TaskInstance getTaskByName(String flowId, String taskName) {
        return getExecutionDAO().getTaskByName(flowId, taskName);
    }

    private void updateResult(final TaskInstance taskInstance) {
        TaskResult taskResult = new TaskResult(
                taskInstance.getFlowId(),
                taskInstance.getTaskId(),
                taskInstance.getStatus()
        );
        taskResult.setOutput(taskInstance.getOutput());
        taskResult.setProgress(taskInstance.getProgress());
        taskResult.setExtension(taskInstance.getExtension());
        taskResult.setReasonForNotCompleting(
                taskInstance.getReasonForNotCompleting());

        updateTask(taskResult);
    }

    public void updateTask(final TaskResult taskResult) {
        if (!flowLockFacade.acquireLock(taskResult.getFlowId())) {
            throw new FlowException(FLOW_EXECUTION_ERROR,
                    String.format("Error acquiring lock " +
                                    "when update task result, " +
                                    "flowId: %s taskId: %s",
                            taskResult.getFlowId(),
                            taskResult.getTaskId()));
        }

        try {
            updateTaskUnsafe(taskResult);
        } finally {
            flowLockFacade.releaseLock(taskResult.getFlowId());
        }
    }

    private void updateTaskUnsafe(final TaskResult taskResult) {
        FlowInstance flowInstance = getFlow(taskResult.getFlowId());
        if (flowInstance == null) {
            throw new FlowException(FLOW_NOT_EXIST,
                    String.format("No such flow found by flowId: %s",
                            taskResult.getFlowId()));
        }

        if (flowInstance.getStatus().isTerminal()) {
            log.info("Flow: {} has already finished execution. " +
                            "Task update for: {} ignored.",
                    taskResult.getFlowId(), taskResult.getTaskId());
            return;
        }

        fillDef(flowInstance);

        TaskInstance taskInstance = flowInstance.getTaskById(taskResult.getTaskId())
                .orElseThrow(() -> new FlowException(TASK_NOT_EXIST,
                        String.format("No such task found by taskId: %s",
                                taskResult.getTaskId())));

        TaskStatus status = taskInstance.getStatus();
        if (status.isTerminal()
                && !status.isHanged()
                && !status.isRetried()) {
            return;
        }

        taskInstance.setOutput(taskResult.getOutput());
        taskInstance.setStatus(taskResult.getStatus());
        taskInstance.setProgress(taskResult.getProgress());
        taskInstance.setReasonForNotCompleting(taskResult.getReasonForNotCompleting());
        taskInstance.setEndTime(TimeUtils.currentTimeMillis());

        try {
            FlowContext.setCurrentFlow(flowInstance);
            updateTask(taskInstance);
        } finally {
            FlowContext.removeCurrentFlow();
        }

        executePerfectlyUnsafe(flowInstance);
    }

    private List<TaskInstance> createTasks(final List<TaskInstance> taskInstances) {
        return getExecutionDAO().createTasks(taskInstances);
    }

    private void updateTasks(final List<TaskInstance> taskInstances) {
        if (CollectionUtils.isEmpty(taskInstances)) {
            return;
        }

        taskInstances.forEach(taskInstance -> {
            fillTask(taskInstance);
            terminal(taskInstance);
        });

        try {
            getExecutionDAO().updateTasks(taskInstances);
        } catch (Throwable throwable) {
            String errorMsg = String
                    .format(
                            "Error updating tasks: %s in flow: %s",
                            Joiner
                                    .on(Delimiter.COMMA)
                                    .join(taskInstances
                                            .stream()
                                            .map(TaskInstance::getTaskId)
                                            .collect(Collectors.toList())),
                            taskInstances.get(0).getFlowId());
            log.error(errorMsg, throwable);
            throw throwable;
        }
    }

    private void updateTask(final TaskInstance taskInstance) {
        fillTask(taskInstance);
        terminal(taskInstance);

        try {
            getExecutionDAO().updateTask(taskInstance);
        } catch (Throwable throwable) {
            String errorMsg = String
                    .format(
                            "Error updating task: %s in flow: %s",
                            taskInstance.getTaskId(),
                            taskInstance.getFlowId());
            log.error(errorMsg, throwable);
            throw throwable;
        }
    }

    private void terminal(final TaskInstance taskInstance) {
        TaskStatus status = taskInstance.getStatus();
        if (!status.isTerminal()
                || status.isHanged()
                || status.isRetried()
                || taskInstance.isExecuted()) {
            return;
        }

        taskAspect.onTerminated(taskInstance);
    }

    private void deleteTask(String taskId) {
        getExecutionDAO().deleteTask(taskId);
    }

    private void fillDef(final FlowInstance flowInstance) {
        if (flowInstance.getFlowDef() == null) {
            Optional.ofNullable(metadataService.getFlow(
                            flowInstance.getFlowName()))
                    .ifPresent(flowInstance::setFlowDef);
        }
    }

    private void fillTask(final TaskInstance taskInstance) {
        if (taskInstance.getStatus() != null) {

            taskInstance.setLastUpdated(TimeUtils.currentTimeMillis());

            if (taskInstance.getStatus().isTerminal()
                    && taskInstance.getEndTime() == 0) {
                taskInstance.setEndTime(TimeUtils.currentTimeMillis());
            }
        }
    }

    private boolean checkForFlowCompletion(final FlowInstance flowInstance) {
        if (CollectionUtils.isEmpty(flowInstance.getTaskInstances())) {
            return false;
        }

        final Map<String, TaskStatus> taskStatusMap =
                flowInstance.getTaskInstances()
                        .stream()
                        .collect(Collectors.toMap(
                                TaskInstance::getTaskName,
                                TaskInstance::getStatus));

        List<TaskDef> taskDefs =
                flowInstance.getFlowDef().getTaskDefs();

        boolean allCompletedSuccessfully = taskDefs.stream()
                .allMatch(taskDef -> {
                    String taskName = taskDef.getName();
                    if (flowInstance.getSkipTasks().contains(taskName)) {
                        return true;
                    }
                    TaskStatus status = taskStatusMap.get(taskDef.getName());
                    return status != null && status.isFinished();
                });

        if (!allCompletedSuccessfully) {
            return false;
        }

        boolean noPendingTasks = taskStatusMap.values().stream()
                .allMatch(TaskStatus::isTerminal);

        if (!noPendingTasks) {
            return false;
        }

        return flowInstance.getTaskInstances().stream()
                .noneMatch(taskInstance -> {
                    TaskDef next = getNextTask(
                            flowInstance, taskInstance.getTaskDef());
                    return next != null
                            && !taskStatusMap.containsKey(next.getName());
                });
    }

    private static FlowInstance newFlowInstance(final StartFlowReq startFlowReq) {
        FlowDef flowDef = requireNonNull(startFlowReq.getFlowDef());
        FlowInstance flowInstance = FlowInstance.create(flowDef);
        flowInstance.setParentFlowId(startFlowReq.getParentFlowId());
        flowInstance.setParentTaskId(startFlowReq.getParentTaskId());
        flowInstance.setCorrelationId(startFlowReq.getCorrelationId());
        flowInstance.setInput(getFlowInput(flowDef,
                startFlowReq.getInput(), startFlowReq.getExtension()));
        flowInstance.setExtension(startFlowReq.getExtension());
        return flowInstance;
    }

    private static long remainWaitTime(long expectedInterval, long beginTimestampMs) {
        long hasPassed = TimeUtils.currentTimeMillis() - beginTimestampMs;
        return hasPassed < 0 ? expectedInterval : expectedInterval - hasPassed;
    }

    private void checkConcurrency(final FlowInstance flowInstance) {
        FlowDef flowDef = flowInstance.getFlowDef();
        if (flowDef != null && flowDef.getControlDef() != null) {
            FlowDef.ControlDef controlDef = flowDef.getControlDef();
            Integer concurrencyLimit = controlDef.getConcurrencyLimit();
            if (concurrencyLimit != null && 0 < concurrencyLimit) {
                List<String> runningFlowIds =
                        getExecutionDAO().getRunningFlowIds(flowDef.getName());
                if (CollectionUtils.isNotEmpty(runningFlowIds)
                        && concurrencyLimit < runningFlowIds.size()) {
                    throw new FlowException(CONCURRENCY_LIMIT,
                            String.format("Trigger concurrency limit, limit: %d current: %d",
                                    concurrencyLimit, runningFlowIds.size()));
                }
            }
        }
    }

    private boolean checkConcurrency(final TaskInstance taskInstance) {
        TaskDef taskDef = taskInstance.getTaskDef();
        if (taskDef != null && taskDef.getControlDef() != null) {
            TaskDef.ControlDef controlDef = taskDef.getControlDef();
            Integer concurrencyLimit = controlDef.getConcurrencyLimit();
            if (concurrencyLimit != null && 0 < concurrencyLimit) {
                List<String> runningTaskIds =
                        getExecutionDAO().getRunningTaskIds(taskDef.getName());
                if (CollectionUtils.isNotEmpty(runningTaskIds)
                        && concurrencyLimit < runningTaskIds.size()
                        && !runningTaskIds.contains(taskInstance.getTaskId())) {
                    log.warn("Task: {} trigger concurrency limit, limit: {} current: {}",
                            taskInstance.getTaskId(),
                            concurrencyLimit,
                            runningTaskIds.size());
                    return true;
                }
            }
        }
        return false;
    }

    private FlowInstance findLastFailedIfAny(final FlowInstance flowInstance) {
        Optional<TaskInstance> taskToRetry = flowInstance
                .getTaskInstances()
                .stream()
                .filter(__ ->
                        __.getStatus()
                                .isUnsuccessfullyTerminated())
                .findFirst();

        return taskToRetry.map(taskInstance ->
                        findFirstFailedIfAny(taskInstance, flowInstance))
                .orElse(null);
    }

    private FlowInstance findFirstFailedIfAny(final TaskInstance taskInstance,
                                              final FlowInstance parentFlow) {
        if (StringUtils.isNotBlank(taskInstance.getSubFlowId())) {

            if (taskInstance.getStatus().isTerminal()
                    && !taskInstance.getStatus().isSuccessful()) {

                FlowInstance subFlow = getFlow(taskInstance.getSubFlowId());

                Optional<TaskInstance> taskToRetry = subFlow
                        .getTaskInstances()
                        .stream()
                        .filter(__ ->
                                __.getStatus()
                                        .isUnsuccessfullyTerminated())
                        .findFirst();

                if (taskToRetry.isPresent()) {
                    return findFirstFailedIfAny(taskToRetry.get(), subFlow);
                }
            }
        }

        return parentFlow;
    }

    private ExecutionDAO getExecutionDAO() {
        return getExecutionDAO(null);
    }

    private ExecutionDAO getExecutionDAO(FlowInstance flowInstance) {
        String protocol = executionProperties.getProtocol();
        if (flowInstance == null) {
            flowInstance = FlowContext.getCurrentFlow();
        }
        if (flowInstance != null && flowInstance.getFlowDef() != null) {
            FlowDef flowDef = flowInstance.getFlowDef();
            if (flowDef.getControlDef() != null) {
                FlowDef.ControlDef controlDef = flowDef.getControlDef();
                if (StringUtils.isNotBlank(controlDef.getExecutionProtocol())) {
                    protocol = controlDef.getExecutionProtocol();
                }
            }
        }
        return ExtensionLoader.getExtension(ExecutionDAO.class, protocol);
    }

    private QueueService getQueueService() {
        String protocol = queueProperties.getProtocol();
        final FlowInstance flowInstance = FlowContext.getCurrentFlow();
        if (flowInstance != null && flowInstance.getFlowDef() != null) {
            FlowDef flowDef = flowInstance.getFlowDef();
            if (flowDef.getControlDef() != null) {
                FlowDef.ControlDef controlDef = flowDef.getControlDef();
                if (StringUtils.isNotBlank(controlDef.getQueueProtocol())) {
                    protocol = controlDef.getQueueProtocol();
                }
            }
        }

        QueueService queueService =
                ExtensionLoader.getExtension(QueueService.class, protocol);

        if (queueService != null) {
            DelayedTaskMonitor.init(queueService, this, flowLockFacade, delayedTaskMonitorProperties);
        }

        return queueService;
    }

    @Data
    private static class DecideResult {

        boolean complete;

        List<TaskInstance> tasksToBeScheduled = new LinkedList<>();

        List<TaskInstance> tasksToBeUpdated = new LinkedList<>();

        List<TaskInstance> tasksToBeRetried = new LinkedList<>();
    }

    private interface RequestCallback {

        void callback();
    }
}
