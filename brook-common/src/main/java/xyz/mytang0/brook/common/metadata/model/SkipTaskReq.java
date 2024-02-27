package xyz.mytang0.brook.common.metadata.model;

import lombok.Data;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;

@Data
public class SkipTaskReq implements Serializable {

    private static final long serialVersionUID = -4243497912577689761L;

    @Nonnull
    @NotBlank
    private String flowId;

    @Nonnull
    @NotBlank
    private String taskName;

    private Object input;

    private Object output;
}
