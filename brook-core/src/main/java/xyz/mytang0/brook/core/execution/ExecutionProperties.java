package xyz.mytang0.brook.core.execution;

import lombok.Data;
import xyz.mytang0.brook.spi.config.ConfigProperties;

@ConfigProperties(prefix = "brook.execution-dao")
@Data
public class ExecutionProperties {

    /**
     * The protocol of the execution-dao.
     */
    private String protocol;

    /**
     * Whether parallel task execution is enabled.
     * When false, PARALLEL task branches execute sequentially.
     */
    private boolean parallelEnabled = true;
}
