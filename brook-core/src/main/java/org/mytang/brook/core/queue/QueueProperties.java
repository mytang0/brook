package org.mytang.brook.core.queue;

import lombok.Data;
import org.mytang.brook.core.constants.FlowConstants;

@Data
public class QueueProperties {

    /**
     * The protocol of the queue-service. The default is 'local'.
     */
    private String protocol = FlowConstants.QUEUE_DEFAULT;
}
