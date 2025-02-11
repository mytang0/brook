package xyz.mytang0.brook.common.metadata.model;

import lombok.Data;
import xyz.mytang0.brook.common.metadata.enums.TaskStatus;
import xyz.mytang0.brook.common.metadata.extension.Extension;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;

@Data
public class TaskResult implements Serializable {

    private static final long serialVersionUID = -297066207396697368L;

    @Nonnull
    @NotBlank
    private String flowId;

    @Nonnull
    @NotBlank
    private String taskId;

    @Nonnull
    private TaskStatus status;

    private String reasonForNotCompleting;

    private Object output;

    private Integer progress;

    private Extension extension;
}
