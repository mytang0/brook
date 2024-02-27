package xyz.mytang0.brook.common.metadata.definition;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static xyz.mytang0.brook.common.metadata.definition.TaskDef.RetryLogic.FIXED;
import static xyz.mytang0.brook.common.metadata.definition.TaskDef.TimeoutPolicy.TIME_OUT;

@Data
public class TaskDef implements Serializable {

    private static final long serialVersionUID = -8797791731692389319L;

    @NotBlank
    private String type;

    @NotBlank
    private String name;

    private String display;

    private String description;

    @Valid
    private ControlDef controlDef;

    @Valid
    private ProgressDef progressDef;

    @Valid
    private LogDef logDef;

    @Valid
    private LinkDef linkDef;

    @Valid
    private CheckDef checkDef;

    @Valid
    private HangDef hangDef;

    @Valid
    private Callback callback;

    private Extension extension;

    private Object template;

    private Object input;

    private Object output;

    @JsonIgnore
    // Performance optimization items, does not contain any instance data.
    private transient Object parsed;

    @JsonIgnore
    @SuppressWarnings("unchecked")
    public <T> T getParsed() {
        return (T) parsed;
    }

    // Do not delete.
    public void setTemplate(Object template) {
        if (template instanceof Map) {
            this.template = template;
        } else if (template instanceof List) {
            this.template = template;
        } else if (template instanceof String) {
            this.template = template;
        } else if (template != null) {
            throw new IllegalArgumentException("illegal task def template type, " +
                    "only map, list and string are supported.");
        }
    }

    // Do not delete.
    public void setInput(Object input) {
        if (input instanceof Map) {
            this.input = input;
        } else if (input instanceof List) {
            this.input = input;
        } else if (input instanceof String) {
            this.input = input;
        } else if (input != null) {
            throw new IllegalArgumentException("illegal task def input type, " +
                    "only map, list and string are supported.");
        }
    }

    // Do not delete.
    public void setOutput(Object output) {
        if (output instanceof Map) {
            this.output = output;
        } else if (output instanceof List) {
            this.output = output;
        } else if (output instanceof String) {
            this.output = output;
        } else if (output != null) {
            throw new IllegalArgumentException("illegal task def output type, " +
                    "only map, list and string are supported.");
        }
    }

    public String getExtension(String key) {
        return this.getExtension(key, null);
    }

    public String getExtension(String key, String defaultValue) {
        return Optional.ofNullable(extension)
                .map(extension -> extension.get(key))
                .orElse(defaultValue);
    }

    @Data
    public static class ControlDef implements Serializable {

        private static final long serialVersionUID = 2707638089552805654L;

        // Skip current task condition configuration.
        private SkipDef skipDef;

        private long startDelayMs;

        // Timeout configuration.
        private long timeoutMs;

        private TimeoutPolicy timeoutPolicy = TIME_OUT;

        // Retry configuration.
        private int retryCount;

        private long retryDelayMs;

        private RetryLogic retryLogic = FIXED;

        // Enable output cache.
        private Boolean enableCache;

        private Integer concurrencyLimit;

        public void setStartDelayMs(long startDelayMs) {
            if (startDelayMs < 0) {
                throw new IllegalArgumentException(
                        "Illegal startDelayMs: " + startDelayMs);
            }
            this.startDelayMs = startDelayMs;
        }

        public void setTimeoutMs(long timeoutMs) {
            if (timeoutMs < 0) {
                throw new IllegalArgumentException(
                        "Illegal timeoutMs: " + timeoutMs);
            }
            this.timeoutMs = timeoutMs;
        }

        public void setRetryDelayMs(long retryDelayMs) {
            if (retryDelayMs < 0) {
                throw new IllegalArgumentException(
                        "Illegal retryDelayMs: " + retryDelayMs);
            }
            this.retryDelayMs = retryDelayMs;
        }
    }

    @Data
    public static class SkipDef implements Serializable {

        private static final long serialVersionUID = 1178463667953694465L;

        private String engineType;

        // When the condition is met, skip the current task.
        private String skipCondition;
    }

    @Data
    public static class LogDef implements Serializable {

        private static final long serialVersionUID = -6438574393474880132L;

        private String startFormat;

        private String terminalFormat;
    }

    @Data
    public static class ProgressDef implements Serializable {

        private static final long serialVersionUID = -4931785414169962135L;

        private Integer progress;

        private Integer interval;
    }

    @Data
    public static class LinkDef implements Serializable {

        private static final long serialVersionUID = 6733876571225316756L;

        @NotBlank
        private String title;

        @NotBlank
        private String url;
    }

    @Data
    public static class Callback implements Serializable {

        private static final long serialVersionUID = 9163835454092236395L;

        private boolean async = false;

        private String protocol;

        private Object input;
    }

    @Data
    public static class CheckDef implements Serializable {

        private static final long serialVersionUID = 2576648439299308136L;

        private String engineType;

        /**
         * Check whether the task was successful from a business perspective.
         */
        @Valid
        private SuccessDef successDef;

        /**
         * Check whether the task needs to be retried from a business perspective.
         */
        @Valid
        private RetryDef retryDef;
    }

    @Data
    public static class SuccessDef implements Serializable {

        private static final long serialVersionUID = -4574313421729400417L;

        @NotBlank
        private String successCondition;

        private String failureReasonExpression;
    }

    @Data
    public static class RetryDef implements Serializable {

        private static final long serialVersionUID = -3705851293651586214L;

        @NotBlank
        private String retryCondition;
    }

    @Data
    public static class HangDef implements Serializable {

        private static final long serialVersionUID = -8533869821164523444L;

        /**
         * The current task is an asynchronous task, and when the task returns,
         * it may actually be executing asynchronously.
         * This is why we need a check task to check periodically to determine
         * whether the asynchronous task is completed.
         */
        @Valid
        @NotNull
        private TaskDef determineTaskDef;

        /**
         * Feedback the output of determine task to parent-task instance.
         */
        private boolean feedbackOutput;
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class Extension extends HashMap<String, String> {

        private static final long serialVersionUID = 6799894392838843930L;

        public Extension() {
            super();
        }

        public Extension(Map<String, String> extension) {
            super(extension);
        }
    }

    public enum TimeoutPolicy {

        @JsonAlias({"TIME_OUT", "TIME_OUT_WF"})
        TIME_OUT,

        ALERT_ONLY,

        RETRY,
    }

    public enum RetryLogic {
        FIXED,

        EXPONENTIAL_BACKOFF
    }
}
