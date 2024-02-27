package xyz.mytang0.brook.common.metadata.definition;

import xyz.mytang0.brook.common.utils.JsonUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.io.Serializable;
import java.util.List;

import static xyz.mytang0.brook.common.metadata.definition.FlowDef.TimeoutPolicy.ALERT_ONLY;

@Data
public class FlowDef implements Serializable {

    private static final long serialVersionUID = -5000110478110622690L;

    @NotBlank
    private String name;

    private Integer version;

    private String description;

    @Valid
    private ControlDef controlDef;

    @NotEmpty
    private List<@Valid TaskDef> taskDefs;

    private Object input;

    private Object output;

    private String failureFlowName;

    @JsonIgnore
    public FlowDef copy() {
        return JsonUtils.readValue(
                JsonUtils.toJsonString(this),
                FlowDef.class);
    }

    @Data
    public static class ControlDef implements Serializable {

        private static final long serialVersionUID = 5258381155432450874L;

        private long timeoutMs;

        private TimeoutPolicy timeoutPolicy = ALERT_ONLY;

        private Boolean enableLog;

        private Integer concurrencyLimit;

        private String executionProtocol;

        private String queueProtocol;
    }

    public enum TimeoutPolicy {
        TIME_OUT,

        ALERT_ONLY
    }
}
