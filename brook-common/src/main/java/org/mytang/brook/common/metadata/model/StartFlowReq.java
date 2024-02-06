package org.mytang.brook.common.metadata.model;

import org.mytang.brook.common.metadata.definition.FlowDef;
import org.mytang.brook.common.metadata.extension.Extension;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StartFlowReq implements Serializable {

    private static final long serialVersionUID = 8677036466861630616L;

    @NotBlank
    private String name;

    private Integer version;

    @Valid
    private FlowDef flowDef;

    private Object input;

    private String correlationId;

    private String parentFlowId;

    private String parentTaskId;

    private Extension extension;
}
