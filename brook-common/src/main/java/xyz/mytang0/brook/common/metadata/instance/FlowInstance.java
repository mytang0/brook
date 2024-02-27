package xyz.mytang0.brook.common.metadata.instance;

import xyz.mytang0.brook.common.holder.UserHolder;
import xyz.mytang0.brook.common.metadata.definition.FlowDef;
import xyz.mytang0.brook.common.metadata.enums.FlowStatus;
import xyz.mytang0.brook.common.metadata.extension.Extension;
import xyz.mytang0.brook.common.metadata.model.User;
import xyz.mytang0.brook.common.utils.IDUtils;
import xyz.mytang0.brook.common.utils.StringUtils;
import xyz.mytang0.brook.common.utils.TimeUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Data
public class FlowInstance implements Serializable {

    private static final long serialVersionUID = -1733036848209780812L;

    private String flowId;

    private String flowName;

    private Integer flowVersion;

    @JsonIgnore
    private FlowDef flowDef;

    private FlowStatus status;

    private String reasonForNotCompleting;

    private String correlationId;

    private String parentFlowId;

    private String parentTaskId;

    private String failureFlowId;

    private Set<String> skipTasks = new HashSet<>();

    private List<TaskInstance> taskInstances = new ArrayList<>();

    private Object input;

    private Object output;

    private User creator;

    private volatile Extension extension;

    private String logAddress;

    private long startTime;

    private long lastUpdated;

    private long endTime;

    public Optional<TaskInstance> getTaskByName(String taskName) {
        return taskInstances.stream()
                .filter(task -> task.getTaskName().equals(taskName))
                .findFirst();
    }

    public Optional<TaskInstance> getTaskById(String taskId) {
        return taskInstances.stream()
                .filter(task -> task.getTaskId().equals(taskId))
                .findFirst();
    }

    public String getExtension(String key) {
        return getExtension(key, null);
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

    public boolean hasParent() {
        return StringUtils.isNotBlank(parentFlowId);
    }

    public static FlowInstance create(FlowDef flowDef) {
        FlowInstance flowInstance = new FlowInstance();
        flowInstance.setFlowId(IDUtils.generator(flowDef));
        flowInstance.setFlowName(flowDef.getName());
        flowInstance.setFlowVersion(flowDef.getVersion());
        flowInstance.setFlowDef(flowDef);
        flowInstance.setStartTime(TimeUtils.currentTimeMillis());
        flowInstance.setStatus(FlowStatus.RUNNING);
        flowInstance.setCreator(UserHolder.getCurrentUser());
        return flowInstance;
    }
}
