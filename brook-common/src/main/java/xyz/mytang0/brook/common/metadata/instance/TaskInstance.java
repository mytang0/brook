package xyz.mytang0.brook.common.metadata.instance;

import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.extension.Extension;
import xyz.mytang0.brook.common.utils.IDUtils;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.common.utils.TimeUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import static xyz.mytang0.brook.common.utils.FieldUtils.getProperty;

@Data
public class TaskInstance implements Serializable {

    private static final long serialVersionUID = -7439645031194887772L;

    private String taskId;

    private String flowId;

    private String taskName;

    private TaskDef taskDef;

    private TaskStatus status;

    private String reasonForNotCompleting;

    private Object input;

    private Object output;

    private String subFlowId;

    private String parentTaskId;

    private String hangTaskId;

    private Integer progress;

    private Link link;

    private volatile Extension extension;

    private boolean executed;

    private boolean hanging;

    private long scheduledTime;

    private long startTime;

    private long lastUpdated;

    private long endTime;

    // Start related.
    private long startDelayMs;

    private long retryTime;

    private int retryCount;


    @JsonIgnore
    @SuppressWarnings("unchecked")
    public Configuration getInputConfiguration() {
        if (input instanceof Map) {
            return new Configuration((Map<String, Object>) input);
        }
        throw new IllegalArgumentException();
    }

    @SuppressWarnings("unchecked")
    public <T> T getInput() {
        return (T) input;
    }

    @SuppressWarnings("unchecked")
    public <T> T getOutput() {
        return (T) output;
    }

    /**
     * Rewrite to avoid exception 'retryCount'.
     */
    public int getRetryCount() {
        return Math.max(retryCount, 0);
    }

    public TaskInstance copy() {
        TaskInstance copy = new TaskInstance();
        copy.setTaskId(taskId);
        copy.setFlowId(flowId);
        copy.setTaskName(taskName);
        copy.setTaskDef(taskDef);
        copy.setStatus(status);
        copy.setReasonForNotCompleting(reasonForNotCompleting);
        copy.setInput(input);
        copy.setExtension(extension);
        return copy;
    }

    @JsonIgnore
    public boolean isRetryable() {
        return getStatus().isRetryable()
                && getTaskDef().getControlDef() != null
                && getTaskDef().getControlDef().getRetryCount() > getRetryCount();
    }

    public <T> T getInput(Class<T> toValueType) {
        return JsonUtils.convertValue(input, toValueType);
    }

    public <T> T getInput(TypeReference<T> toValueTypeRef) {
        return JsonUtils.convertValue(input, toValueTypeRef);
    }

    public <T> T getOutput(Class<T> toValueType) {
        return JsonUtils.convertValue(output, toValueType);
    }

    public <T> T getOutput(TypeReference<T> toValueTypeRef) {
        return JsonUtils.convertValue(output, toValueTypeRef);
    }

    public <T> T getPropertyFromInput(String name) {
        return getProperty(input, name);
    }

    public <T> T getPropertyFromOutput(String name) {
        return getProperty(output, name);
    }

    public String getExtension(String key) {
        return this.getExtension(key, null);
    }

    public String getExtension(String key, String defaultValue) {
        return Optional.ofNullable(extension)
                .map(extension -> extension.get(key))
                .orElse(defaultValue);
    }

    public void setExtension(String key, String value) {
        initExtension();
        extension.put(key, value);
    }

    public void setExtensionIfAbsent(String key, String value) {
        initExtension();
        extension.putIfAbsent(key, value);
    }

    private void initExtension() {
        if (extension == null) {
            synchronized (this) {
                if (extension == null) {
                    extension = new Extension();
                }
            }
        }
    }

    public static TaskInstance create(TaskDef taskDef) {
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setTaskId(IDUtils.generator(taskDef));
        taskInstance.setTaskName(taskDef.getName());
        taskInstance.setTaskDef(taskDef);
        taskInstance.setStatus(TaskStatus.SCHEDULED);
        taskInstance.setScheduledTime(TimeUtils.currentTimeMillis());
        Optional.ofNullable(taskDef.getControlDef())
                .ifPresent(controlDef ->
                        taskInstance.setStartDelayMs(controlDef.getStartDelayMs())
                );
        return taskInstance;
    }

    @Data
    public static class Link implements Serializable {

        private static final long serialVersionUID = -3191821201740213790L;

        private String title;

        private String url;
    }
}
