package xyz.mytang0.brook.core.queue;

import lombok.Data;
import xyz.mytang0.brook.core.constants.FlowConstants;
import xyz.mytang0.brook.spi.config.ConfigProperties;

@ConfigProperties(prefix = "brook.queue")
@Data
public class QueueProperties {

    /**
     * The protocol of the queue-service. The default is 'local'.
     */
    private String protocol = FlowConstants.QUEUE_DEFAULT;
}
