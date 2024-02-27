package xyz.mytang0.brook.common.context;

import xyz.mytang0.brook.common.configuration.Configuration;
import xyz.mytang0.brook.common.metadata.definition.TaskDef;
import xyz.mytang0.brook.common.metadata.instance.FlowInstance;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class TaskMapperContext {

    private final FlowInstance flowInstance;

    private final TaskDef taskDef;

    private final Object input;

    @SuppressWarnings("unchecked")
    public Configuration getInputConfiguration() {
        if (input instanceof Configuration) {
            return (Configuration) input;
        } else if (input instanceof Map) {
            return new Configuration((Map<String, Object>) input);
        }
        throw new IllegalArgumentException("Input is not a map type!");
    }
}
